package imagestore_test

import (
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/bakins/test-helpers"
	"github.com/mistifyio/go-zfs"
	"github.com/mistifyio/mistify-agent-image"
	"github.com/mistifyio/mistify-agent/rpc"
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
		defer f.Close()
		err := f.Truncate(pow2(30))
		helpers.Ok(t, err)
		tempfiles[i] = f.Name()
		defer os.Remove(f.Name())
	}

	pool, err := zfs.CreateZpool("test", nil, tempfiles...)
	helpers.Ok(t, err)
	defer pool.Destroy()

	store, err := imagestore.Create(imagestore.Config{Zpool: "test"})
	helpers.Ok(t, err)
	go store.Run()
	defer store.Destroy()

	fn(store, t)
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
		request := &rpc.VolumeRequest{
			Size: 64,
			Id:   "test/test-volume",
		}
		err := store.CreateVolume(&http.Request{}, request, response)
		helpers.Ok(t, err)
		helpers.Equals(t, 1, len(response.Volumes))
	})
}
