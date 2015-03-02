package imagestore_test

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bakins/test-helpers"
	"github.com/mistifyio/mistify-agent-image"
	"github.com/mistifyio/mistify-agent/rpc"
	"gopkg.in/mistifyio/go-zfs.v1"
)

var defaultZfsOptions map[string]string = map[string]string{
	"compression": "lz4",
}

var (
	zpool      string = "test"
	parentName string = "testParent"
	childName  string = "testChild"
)

func getParentDatasetId(withZpool bool) string {
	if withZpool {
		return filepath.Join(zpool, parentName)
	} else {
		return parentName
	}
}

func getChildDatasetId(withZpool bool) string {
	return filepath.Join(getParentDatasetId(withZpool), childName)
}

func getParentSnapshotId(snapshotName string, withZpool bool) string {
	return fmt.Sprintf("%s@%s", getParentDatasetId(withZpool), snapshotName)
}

func getChildSnapshotId(snapshotName string, withZpool bool) string {
	return fmt.Sprintf("%s@%s", getChildDatasetId(withZpool), snapshotName)
}

func missingIdParam(t *testing.T, fn func(*http.Request, *rpc.SnapshotRequest, *rpc.SnapshotResponse) error) {
	response := &rpc.SnapshotResponse{}
	request := &rpc.SnapshotRequest{}
	err := fn(&http.Request{}, request, response)
	helpers.Equals(t, "need an id", err.Error())
}

func notFoundIdParam(t *testing.T, fn func(*http.Request, *rpc.SnapshotRequest, *rpc.SnapshotResponse) error) {
	response := &rpc.SnapshotResponse{}
	request := &rpc.SnapshotRequest{
		Id: "querty",
	}
	err := fn(&http.Request{}, request, response)
	helpers.Equals(t, imagestore.ErrNotFound, err)
}

func notValidIdParam(t *testing.T, fn func(*http.Request, *rpc.SnapshotRequest, *rpc.SnapshotResponse) error) {
	response := &rpc.SnapshotResponse{}
	request := &rpc.SnapshotRequest{
		Id: "querty@",
	}
	err := fn(&http.Request{}, request, response)
	helpers.Equals(t, imagestore.ErrNotValid, err)
}

func notSnapshotIdParam(t *testing.T, fn func(*http.Request, *rpc.SnapshotRequest, *rpc.SnapshotResponse) error) {
	response := &rpc.SnapshotResponse{}
	request := &rpc.SnapshotRequest{
		Id: parentName,
	}
	err := fn(&http.Request{}, request, response)
	helpers.Equals(t, imagestore.ErrNotSnapshot, err)
}

func testIdParam(t *testing.T, fn func(*http.Request, *rpc.SnapshotRequest, *rpc.SnapshotResponse) error, requireSnapshot bool) {
	missingIdParam(t, fn)
	notFoundIdParam(t, fn)
	notValidIdParam(t, fn)

	if requireSnapshot {
		notSnapshotIdParam(t, fn)
	}
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
		checkSnapshotResults(t, response, getParentSnapshotId(snapshotName, true), getChildSnapshotId(snapshotName, true))
	} else {
		checkSnapshotResults(t, response, getParentSnapshotId(snapshotName, true))
	}

	// Sleep to minimize name collisions
	sleep(1)

	return snapshotName
}

func withFilesystems(t *testing.T, fn func(store *imagestore.ImageStore, t *testing.T)) {
	withImageStore(t, func(store *imagestore.ImageStore, t *testing.T) {
		_, err := zfs.CreateFilesystem(getParentDatasetId(true), defaultZfsOptions)
		helpers.Ok(t, err)
		_, err = zfs.CreateFilesystem(getChildDatasetId(true), defaultZfsOptions)
		helpers.Ok(t, err)
		fn(store, t)
	})
}

func TestCreateSnapshot(t *testing.T) {
	withFilesystems(t, func(store *imagestore.ImageStore, t *testing.T) {
		testIdParam(t, store.CreateSnapshot, false)

		response := &rpc.SnapshotResponse{}
		request := &rpc.SnapshotRequest{
			Id: parentName,
		}

		// No dest
		err := store.CreateSnapshot(&http.Request{}, request, response)
		helpers.Equals(t, "need a dest", err.Error())

		// Invalid dest
		request.Dest = "-?_&"
		err = store.CreateSnapshot(&http.Request{}, request, response)
		helpers.Equals(t, "invalid snapshot dest", err.Error())

		// Successful
		snapshotName := createSnapshot(t, store, false)

		// Snapshot already exists
		request.Dest = snapshotName
		err = store.CreateSnapshot(&http.Request{}, request, response)
		helpers.Assert(t, strings.Contains(err.Error(), "dataset already exists"), "Wrong error for existing snapshot")

		// Snapshot of a snapshot
		request.Id = getParentSnapshotId(snapshotName, false)
		err = store.CreateSnapshot(&http.Request{}, request, response)
		helpers.Equals(t, "cannot create a snapshot of a snapshot", err.Error())
	})
}

func TestCreateSnapshotRecursive(t *testing.T) {
	withFilesystems(t, func(store *imagestore.ImageStore, t *testing.T) {
		_ = createSnapshot(t, store, true)
	})
}

func TestListSnapshots(t *testing.T) {
	withFilesystems(t, func(store *imagestore.ImageStore, t *testing.T) {
		notFoundIdParam(t, store.ListSnapshots)

		response := &rpc.SnapshotResponse{}

		// List on a clean setup
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
		checkSnapshotResults(t, response, getParentSnapshotId(snapshotName, true), getChildSnapshotId(snapshotName, true))

		// List from the descendent
		request = &rpc.SnapshotRequest{
			Id: getChildDatasetId(false),
		}
		err = store.ListSnapshots(&http.Request{}, request, response)
		helpers.Ok(t, err)
		checkSnapshotResults(t, response, getChildSnapshotId(snapshotName, true))
	})
}

func TestGetSnapshot(t *testing.T) {
	withFilesystems(t, func(store *imagestore.ImageStore, t *testing.T) {
		testIdParam(t, store.GetSnapshot, true)
		snapshotName := createSnapshot(t, store, true)

		response := &rpc.SnapshotResponse{}
		request := &rpc.SnapshotRequest{
			Id: getParentSnapshotId(snapshotName, false),
		}

		err := store.GetSnapshot(&http.Request{}, request, response)
		helpers.Ok(t, err)
		checkSnapshotResults(t, response, getParentSnapshotId(snapshotName, true))
	})
}

func TestDeleteSnapshot(t *testing.T) {
	withFilesystems(t, func(store *imagestore.ImageStore, t *testing.T) {
		testIdParam(t, store.DeleteSnapshot, true)

		snapshotName := createSnapshot(t, store, true)

		response := &rpc.SnapshotResponse{}
		request := &rpc.SnapshotRequest{
			Id: getParentSnapshotId(snapshotName, false),
		}

		err := store.DeleteSnapshot(&http.Request{}, request, response)
		helpers.Ok(t, err)
		checkSnapshotResults(t, response, getParentSnapshotId(snapshotName, true))
	})
}

func TestDeleteSnapshotRecursive(t *testing.T) {
	withFilesystems(t, func(store *imagestore.ImageStore, t *testing.T) {
		snapshotName := createSnapshot(t, store, true)

		response := &rpc.SnapshotResponse{}
		request := &rpc.SnapshotRequest{
			Id:        getParentSnapshotId(snapshotName, false),
			Recursive: true,
		}
		err := store.DeleteSnapshot(&http.Request{}, request, response)
		helpers.Ok(t, err)
		checkSnapshotResults(t, response, getParentSnapshotId(snapshotName, true), getChildSnapshotId(snapshotName, true))
	})
}

func TestRollbackSnapshot(t *testing.T) {
	withFilesystems(t, func(store *imagestore.ImageStore, t *testing.T) {
		testIdParam(t, store.DeleteSnapshot, true)

		snapshotName := createSnapshot(t, store, false)

		response := &rpc.SnapshotResponse{}
		request := &rpc.SnapshotRequest{
			Id: getParentSnapshotId(snapshotName, false),
		}
		err := store.RollbackSnapshot(&http.Request{}, request, response)
		helpers.Ok(t, err)
		checkSnapshotResults(t, response, getParentSnapshotId(snapshotName, true))
	})
}

func TestRollbackSnapshotOlder(t *testing.T) {
	withFilesystems(t, func(store *imagestore.ImageStore, t *testing.T) {
		snapshotName1 := createSnapshot(t, store, false)
		_ = createSnapshot(t, store, false)
		response := &rpc.SnapshotResponse{}
		request := &rpc.SnapshotRequest{
			Id:                getParentSnapshotId(snapshotName1, false),
			DestroyMoreRecent: true,
		}
		err := store.RollbackSnapshot(&http.Request{}, request, response)
		helpers.Ok(t, err)
		checkSnapshotResults(t, response, getParentSnapshotId(snapshotName1, true))
	})
}

func testDownload(t *testing.T, store *imagestore.ImageStore, snapshotName string, expectedCode int) {
	postBody := bytes.NewBufferString(fmt.Sprintf(`{"id": "%s"}`, getParentSnapshotId(snapshotName, false)))
	req, err := http.NewRequest("POST", "http://127.0.0.1/snapshots/download", postBody)
	helpers.Ok(t, err)

	w := httptest.NewRecorder()
	store.DownloadSnapshot(w, req)
	helpers.Equals(t, expectedCode, w.Code)
}

func TestDownloadSnapshot(t *testing.T) {
	withFilesystems(t, func(store *imagestore.ImageStore, t *testing.T) {
		snapshotName := createSnapshot(t, store, false)
		testDownload(t, store, "", 400)
		testDownload(t, store, "qwerty", 404)
		testDownload(t, store, snapshotName, 200)
	})
}
