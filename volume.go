package imagestore

import (
	"errors"
	"github.com/mistifyio/go-zfs"
	"github.com/mistifyio/mistify-agent/rpc"
	"net/http"
	"path/filepath"
)

type ()

func deviceForDataset(ds *zfs.Dataset) string {
	return filepath.Join("/dev/zvol", ds.Name)
}

func volumeFromDataset(ds *zfs.Dataset) *rpc.Volume {
	return &rpc.Volume{
		Id:     ds.Name,
		Size:   ds.Volsize / 1024 / 1024,
		Device: deviceForDataset(ds),
	}
}

func (store *ImageStore) ListVolumes(r *http.Request, request *rpc.VolumeRequest, response *rpc.VolumeResponse) error {
	datasets, err := zfs.Volumes(store.config.Zpool)
	if err != nil {
		return err
	}
	volumes := make([]*rpc.Volume, len(datasets))
	for i, _ := range datasets {
		volumes[i] = volumeFromDataset(datasets[i])
	}

	*response = rpc.VolumeResponse{
		Volumes: volumes,
	}
	return nil
}

func (store *ImageStore) CreateVolume(r *http.Request, request *rpc.VolumeRequest, response *rpc.VolumeResponse) error {
	if request.Size >= 0 {
		return errors.New("need a valid size")

	}

	if request.Id == "" {
		return errors.New("need an id")
	}

	ds, err := zfs.CreateVolume(request.Id, request.Size*1024*1024, default_zfs_options)
	if err != nil {
		return err
	}

	*response = rpc.VolumeResponse{
		Volumes: []*rpc.Volume{
			volumeFromDataset(ds),
		},
	}

	return nil
}

func (store *ImageStore) GetVolume(r *http.Request, request *rpc.VolumeRequest, response *rpc.VolumeResponse) error {
	ds, err := zfs.GetDataset(request.Id)
	if err != nil {
		return err
	}
	if ds.Type != "volume" {
		return NotVolume
	}

	*response = rpc.VolumeResponse{
		Volumes: []*rpc.Volume{volumeFromDataset(ds)},
	}
	return nil
}

func (store *ImageStore) DeleteDataset(r *http.Request, request *rpc.VolumeRequest, response *rpc.VolumeResponse) error {
	if request.Id == "" {
		return errors.New("need an id")
	}
	ds, err := zfs.GetDataset(request.Id)
	if err != nil {
	}

	if err := ds.Destroy(true); err != nil {
		return err
	}

	*response = rpc.VolumeResponse{
		Volumes: []*rpc.Volume{volumeFromDataset(ds)},
	}
	return nil
}
