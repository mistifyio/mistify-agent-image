package imagestore_test

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/bakins/test-helpers"
	"github.com/mistifyio/go-zfs"
	"github.com/mistifyio/mistify-agent-image"
	"github.com/mistifyio/mistify-agent/rpc"
)

var default_zfs_options map[string]string = map[string]string{
	"compression": "lz4",
}

// TODO: Make these all more robust instead of relying on human knowledge
// of the test pool and ImageStore defaults

var zpool string = "test"
var parentName string = "testParent"
var childName string = "testChild"

func getParentDatasetId() string {
	return filepath.Join(zpool, parentName)
}
func getChildDatasetId() string {
	return filepath.Join(getParentDatasetId(), childName)
}
func getParentSnapshotId(snapshotName string) string {
	return fmt.Sprintf("%s@%s", getParentDatasetId(), snapshotName)
}
func getChildSnapshotId(snapshotName string) string {
	return fmt.Sprintf("%s@%s", getChildDatasetId(), snapshotName)
}

func checkSnapshotResults(t *testing.T, r *rpc.SnapshotResponse, names ...string) {
	snapshots := r.Snapshots
	helpers.Equals(t, len(names), len(snapshots))
	for i, name := range names {
		helpers.Equals(t, name, snapshots[i].Id)
	}
}

func createSnapshot(t *testing.T, store *imagestore.ImageStore, recursive bool) string {
	snapshotName := fmt.Sprintf("snap-%v", time.Now().Unix())
	response := &rpc.SnapshotResponse{}
	request := &rpc.SnapshotRequest{
		Id:        parentName,
		Dest:      snapshotName,
		Recursive: recursive,
	}
	err := store.CreateSnapshot(&http.Request{}, request, response)
	helpers.Ok(t, err)
	if recursive {
		checkSnapshotResults(t, response, getParentSnapshotId(snapshotName), getChildSnapshotId(snapshotName))
	} else {
		checkSnapshotResults(t, response, getParentSnapshotId(snapshotName))
	}

	// Sleep to minimize name collisions
	sleep(1)

	return snapshotName
}

func withFilesystems(t *testing.T, fn func(store *imagestore.ImageStore, t *testing.T)) {
	withImageStore(t, func(store *imagestore.ImageStore, t *testing.T) {
		_, err := zfs.CreateFilesystem(getParentDatasetId(), default_zfs_options)
		helpers.Ok(t, err)
		_, err = zfs.CreateFilesystem(getChildDatasetId(), default_zfs_options)
		helpers.Ok(t, err)
		fn(store, t)
	})
}

func TestCreateSnapshot(t *testing.T) {
	withFilesystems(t, func(store *imagestore.ImageStore, t *testing.T) {
		_ = createSnapshot(t, store, false)
	})
}

func TestCreateSnapshotRecursive(t *testing.T) {
	withFilesystems(t, func(store *imagestore.ImageStore, t *testing.T) {
		_ = createSnapshot(t, store, true)
	})
}

func TestListSnapshots(t *testing.T) {
	withFilesystems(t, func(store *imagestore.ImageStore, t *testing.T) {
		// List on a clean setup
		response := &rpc.SnapshotResponse{}
		request := &rpc.SnapshotRequest{}
		err := store.ListSnapshots(&http.Request{}, request, response)
		helpers.Ok(t, err)
		checkSnapshotResults(t, response)

		// Create snapshots recursively, with one descendent
		snapshotName := createSnapshot(t, store, true)

		// List from the top level
		request = &rpc.SnapshotRequest{}
		err = store.ListSnapshots(&http.Request{}, request, response)
		helpers.Ok(t, err)
		checkSnapshotResults(t, response, getParentSnapshotId(snapshotName), getChildSnapshotId(snapshotName))

		// List from the descendent
		request = &rpc.SnapshotRequest{
			Id: filepath.Join(parentName, childName),
		}
		err = store.ListSnapshots(&http.Request{}, request, response)
		helpers.Ok(t, err)
		checkSnapshotResults(t, response, getChildSnapshotId(snapshotName))
	})
}

func TestGetSnapshot(t *testing.T) {
	withFilesystems(t, func(store *imagestore.ImageStore, t *testing.T) {
		snapshotName := createSnapshot(t, store, true)

		response := &rpc.SnapshotResponse{}
		request := &rpc.SnapshotRequest{
			Id: fmt.Sprintf("%s@%s", parentName, snapshotName),
		}
		err := store.GetSnapshot(&http.Request{}, request, response)
		helpers.Ok(t, err)
		checkSnapshotResults(t, response, getParentSnapshotId(snapshotName))
	})
}

func TestDeleteSnapshot(t *testing.T) {
	withFilesystems(t, func(store *imagestore.ImageStore, t *testing.T) {
		snapshotName := createSnapshot(t, store, true)

		response := &rpc.SnapshotResponse{}
		request := &rpc.SnapshotRequest{
			Id: fmt.Sprintf("%s@%s", parentName, snapshotName),
		}
		err := store.DeleteSnapshot(&http.Request{}, request, response)
		helpers.Ok(t, err)
		checkSnapshotResults(t, response, getParentSnapshotId(snapshotName))
	})
}

func TestDeleteSnapshotRecursive(t *testing.T) {
	withFilesystems(t, func(store *imagestore.ImageStore, t *testing.T) {
		snapshotName := createSnapshot(t, store, true)

		response := &rpc.SnapshotResponse{}
		request := &rpc.SnapshotRequest{
			Id:        fmt.Sprintf("%s@%s", parentName, snapshotName),
			Recursive: true,
		}
		err := store.DeleteSnapshot(&http.Request{}, request, response)
		helpers.Ok(t, err)
		checkSnapshotResults(t, response, getParentSnapshotId(snapshotName), getChildSnapshotId(snapshotName))
	})
}

func TestRollbackSnapshot(t *testing.T) {
	withFilesystems(t, func(store *imagestore.ImageStore, t *testing.T) {
		snapshotName := createSnapshot(t, store, false)
		response := &rpc.SnapshotResponse{}
		request := &rpc.SnapshotRequest{
			Id: fmt.Sprintf("%s@%s", parentName, snapshotName),
		}
		err := store.RollbackSnapshot(&http.Request{}, request, response)
		helpers.Ok(t, err)
		checkSnapshotResults(t, response, getParentSnapshotId(snapshotName))
	})
}

func TestRollbackSnapshotOlder(t *testing.T) {
	withFilesystems(t, func(store *imagestore.ImageStore, t *testing.T) {
		snapshotName1 := createSnapshot(t, store, false)
		_ = createSnapshot(t, store, false)
		response := &rpc.SnapshotResponse{}
		request := &rpc.SnapshotRequest{
			Id:                fmt.Sprintf("%s@%s", parentName, snapshotName1),
			DestroyMoreRecent: true,
		}
		err := store.RollbackSnapshot(&http.Request{}, request, response)
		helpers.Ok(t, err)
		checkSnapshotResults(t, response, getParentSnapshotId(snapshotName1))
	})
}

func TestDownloadSnapshot(t *testing.T) {
	withFilesystems(t, func(store *imagestore.ImageStore, t *testing.T) {
		snapshotName := createSnapshot(t, store, false)

		postBody := bytes.NewBufferString(fmt.Sprintf(`{"id": "%s@%s"}`, parentName, snapshotName))
		req, err := http.NewRequest("POST", "http://127.0.0.1/snapshots/download", postBody)
		helpers.Ok(t, err)

		w := httptest.NewRecorder()
		store.DownloadSnapshot(w, req)
		helpers.Equals(t, 200, w.Code)
	})
}
