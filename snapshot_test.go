package imagestore_test

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bakins/test-helpers"
	"github.com/mistifyio/mistify-agent-image"
	"github.com/mistifyio/mistify-agent/rpc"
)

var default_zfs_options map[string]string = map[string]string{
	"compression": "lz4",
}

// TODO: Make these all more robust instead of relying on human knowledge
// of the test pool and ImageStore defaults

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
		Id:        "test",
		Dest:      snapshotName,
		Recursive: recursive,
	}
	err := store.CreateSnapshot(&http.Request{}, request, response)
	helpers.Ok(t, err)
	if recursive {
		checkSnapshotResults(t, response, "test@"+snapshotName, "test/images@"+snapshotName)
	} else {
		checkSnapshotResults(t, response, "test@"+snapshotName)
	}

	// Sleep to minimize name collisions
	sleep(1)

	return snapshotName
}

func TestCreateSnapshot(t *testing.T) {
	withImageStore(t, func(store *imagestore.ImageStore, t *testing.T) {
		_ = createSnapshot(t, store, false)
	})
}

func TestCreateSnapshotRecursive(t *testing.T) {
	withImageStore(t, func(store *imagestore.ImageStore, t *testing.T) {
		_ = createSnapshot(t, store, true)
	})
}

func TestListSnapshots(t *testing.T) {
	withImageStore(t, func(store *imagestore.ImageStore, t *testing.T) {
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
		checkSnapshotResults(t, response, "test@"+snapshotName, "test/images@"+snapshotName)

		// List from the descendent
		request = &rpc.SnapshotRequest{
			Id: "test/images",
		}
		err = store.ListSnapshots(&http.Request{}, request, response)
		helpers.Ok(t, err)
		checkSnapshotResults(t, response, "test/images@"+snapshotName)
	})
}

func TestGetSnapshot(t *testing.T) {
	withImageStore(t, func(store *imagestore.ImageStore, t *testing.T) {
		snapshotName := createSnapshot(t, store, true)

		response := &rpc.SnapshotResponse{}
		request := &rpc.SnapshotRequest{
			Id: "test@" + snapshotName,
		}
		err := store.GetSnapshot(&http.Request{}, request, response)
		helpers.Ok(t, err)
		checkSnapshotResults(t, response, "test@"+snapshotName)
	})
}

func TestDeleteSnapshot(t *testing.T) {
	withImageStore(t, func(store *imagestore.ImageStore, t *testing.T) {
		snapshotName := createSnapshot(t, store, true)

		response := &rpc.SnapshotResponse{}
		request := &rpc.SnapshotRequest{
			Id: "test@" + snapshotName,
		}
		err := store.DeleteSnapshot(&http.Request{}, request, response)
		helpers.Ok(t, err)
		checkSnapshotResults(t, response, "test@"+snapshotName)
	})
}

func TestDeleteSnapshotRecursive(t *testing.T) {
	withImageStore(t, func(store *imagestore.ImageStore, t *testing.T) {
		snapshotName := createSnapshot(t, store, true)

		response := &rpc.SnapshotResponse{}
		request := &rpc.SnapshotRequest{
			Id:        "test@" + snapshotName,
			Recursive: true,
		}
		err := store.DeleteSnapshot(&http.Request{}, request, response)
		helpers.Ok(t, err)
		checkSnapshotResults(t, response, "test@"+snapshotName, "test/images@"+snapshotName)
	})
}

func TestRollbackSnapshot(t *testing.T) {
	withImageStore(t, func(store *imagestore.ImageStore, t *testing.T) {
		snapshotName := createSnapshot(t, store, false)
		response := &rpc.SnapshotResponse{}
		request := &rpc.SnapshotRequest{
			Id: "test@" + snapshotName,
		}
		err := store.RollbackSnapshot(&http.Request{}, request, response)
		helpers.Ok(t, err)
		checkSnapshotResults(t, response, "test@"+snapshotName)
	})
}

func TestRollbackSnapshotOlder(t *testing.T) {
	withImageStore(t, func(store *imagestore.ImageStore, t *testing.T) {
		snapshotName1 := createSnapshot(t, store, false)
		_ = createSnapshot(t, store, false)
		response := &rpc.SnapshotResponse{}
		request := &rpc.SnapshotRequest{
			Id:                "test@" + snapshotName1,
			DestroyMoreRecent: true,
		}
		err := store.RollbackSnapshot(&http.Request{}, request, response)
		helpers.Ok(t, err)
		checkSnapshotResults(t, response, "test@"+snapshotName1)
	})
}

func TestDownloadSnapshot(t *testing.T) {
	withImageStore(t, func(store *imagestore.ImageStore, t *testing.T) {
		snapshotName := createSnapshot(t, store, false)

		postBody := bytes.NewBufferString(`{"id": "test@` + snapshotName + `"}`)
		req, err := http.NewRequest("POST", "http://127.0.0.1/snapshots/download", postBody)
		helpers.Ok(t, err)

		w := httptest.NewRecorder()
		store.DownloadSnapshot(w, req)
		helpers.Equals(t, 200, w.Code)
	})
}