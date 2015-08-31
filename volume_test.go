package imagestore_test

import (
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/bakins/test-helpers"
	"github.com/mistifyio/mistify-agent-image"
	"github.com/mistifyio/mistify-agent/rpc"
	logx "github.com/mistifyio/mistify-logrus-ext"
	"gopkg.in/mistifyio/go-zfs.v1"
)

func pow2(x int) int64 {
	return int64(math.Pow(2, float64(x)))
}

func sleep(delay int) {
	time.Sleep(time.Duration(delay) * time.Second)
}

func withImageStore(t *testing.T, fn func(store *imagestore.ImageStore, t *testing.T)) {
	tempfiles := make([]string, 3)
	for i := range tempfiles {
		f, _ := ioutil.TempFile("/tmp/", "zfs-")
		defer logx.LogReturnedErr(f.Close, log.Fields{
			"filename": f.Name(),
		}, "failed to close tempfile")
		err := f.Truncate(pow2(30))
		helpers.Ok(t, err)
		tempfiles[i] = f.Name()
		defer logx.LogReturnedErr(func() error { return os.Remove(f.Name()) },
			log.Fields{"filename": f.Name()},
			"failed to remoev tempfile")
	}

	pool, err := zfs.CreateZpool("test", nil, tempfiles...)
	helpers.Ok(t, err)
	defer logx.LogReturnedErr(pool.Destroy, nil, "unable to destroy zpool")

	store, err := imagestore.Create(imagestore.Config{Zpool: "test"})
	helpers.Ok(t, err)
	go store.Run()
	defer logx.LogReturnedErr(store.Destroy, nil, "unable to destroy imagestore")

	fn(store, t)
}

func createVolume(t *testing.T, store *imagestore.ImageStore) {
	response := &rpc.VolumeResponse{}
	request := &rpc.VolumeRequest{
		ID:   "test-volume",
		Size: 64,
	}
	err := store.CreateVolume(&http.Request{}, request, response)
	helpers.Ok(t, err)
	helpers.Equals(t, 1, len(response.Volumes))
}

func TestListVolumes(t *testing.T) {
	withImageStore(t, func(store *imagestore.ImageStore, t *testing.T) {
		response := &rpc.VolumeResponse{}
		err := store.ListVolumes(&http.Request{}, &rpc.VolumeRequest{}, response)
		helpers.Ok(t, err)
		helpers.Equals(t, 0, len(response.Volumes))
	})
}

func TestCreateVolume(t *testing.T) {
	withImageStore(t, func(store *imagestore.ImageStore, t *testing.T) {
		response := &rpc.VolumeResponse{}
		request := &rpc.VolumeRequest{}

		// Invalid size
		err := store.CreateVolume(&http.Request{}, request, response)
		helpers.Equals(t, "need a valid size", err.Error())

		// Missing ID
		request.Size = 64
		err = store.CreateVolume(&http.Request{}, request, response)
		helpers.Equals(t, "need an id", err.Error())

		createVolume(t, store)
	})
}

func TestGetVolume(t *testing.T) {
	withImageStore(t, func(store *imagestore.ImageStore, t *testing.T) {
		createVolume(t, store)
		_, err := zfs.CreateFilesystem("test/test2", defaultZFSOptions)
		helpers.Ok(t, err)

		response := &rpc.VolumeResponse{}
		request := &rpc.VolumeRequest{}

		// Missing ID
		err = store.GetVolume(&http.Request{}, request, response)
		helpers.Equals(t, "need an id", err.Error())

		// Not a volume
		request.ID = "test2"
		err = store.GetVolume(&http.Request{}, request, response)
		helpers.Equals(t, imagestore.ErrNotVolume, err)

		request.ID = "test-volume"
		err = store.GetVolume(&http.Request{}, request, response)
		helpers.Ok(t, err)
		helpers.Equals(t, 1, len(response.Volumes))
	})
}

func TestDeleteDataset(t *testing.T) {
	withImageStore(t, func(store *imagestore.ImageStore, t *testing.T) {
		createVolume(t, store)
		// 10ms delay to prevent "dataset is busy" error
		time.Sleep(10 * time.Millisecond)

		response := &rpc.VolumeResponse{}
		request := &rpc.VolumeRequest{}

		// Missing ID
		err := store.DeleteDataset(&http.Request{}, request, response)
		helpers.Equals(t, "need an id", err.Error())

		// Not found
		request.ID = "foobar"
		err = store.DeleteDataset(&http.Request{}, request, response)
		helpers.Equals(t, imagestore.ErrNotFound, err)

		// Invalid
		request.ID = "test-volume*"
		err = store.DeleteDataset(&http.Request{}, request, response)
		helpers.Equals(t, imagestore.ErrNotValid, err)

		request.ID = "test-volume"
		err = store.DeleteDataset(&http.Request{}, request, response)
		helpers.Ok(t, err)
		helpers.Equals(t, 1, len(response.Volumes))
	})
}
