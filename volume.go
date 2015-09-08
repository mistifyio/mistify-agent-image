package imagestore

import (
	"errors"
	"net/http"
	"path/filepath"

	"github.com/mistifyio/mistify-agent/rpc"
	"gopkg.in/mistifyio/go-zfs.v1"
)

func deviceForDataset(ds *zfs.Dataset) string {
	return filepath.Join("/dev/zvol", ds.Name)
}

func volumeFromDataset(ds *zfs.Dataset) *rpc.Volume {
	return &rpc.Volume{
		ID:     ds.Name,
		Size:   ds.Volsize / 1024 / 1024,
		Device: deviceForDataset(ds),
	}
}

// ListVolumes lists the zfs volumes
func (store *ImageStore) ListVolumes(r *http.Request, request *rpc.VolumeRequest, response *rpc.VolumeResponse) error {
	datasets, err := zfs.Volumes(store.config.Zpool)
	if err != nil {
		return err
	}
	volumes := make([]*rpc.Volume, len(datasets))
	for i := range datasets {
		volumes[i] = volumeFromDataset(datasets[i])
	}

	*response = rpc.VolumeResponse{
		Volumes: volumes,
	}
	return nil
}

// CreateVolume creates a zfs volume
func (store *ImageStore) CreateVolume(r *http.Request, request *rpc.VolumeRequest, response *rpc.VolumeResponse) error {
	if request.Size <= 0 {
		return errors.New("need a valid size")

	}

	if request.ID == "" {
		return errors.New("need an id")
	}

	fullID := filepath.Join(store.config.Zpool, request.ID)
	ds, err := zfs.CreateVolume(fullID, request.Size*1024*1024, defaultZFSOptions)
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

// GetVolume gets information about a zfs volume
func (store *ImageStore) GetVolume(r *http.Request, request *rpc.VolumeRequest, response *rpc.VolumeResponse) error {
	if request.ID == "" {
		return errors.New("need an id")
	}

	fullID := filepath.Join(store.config.Zpool, request.ID)
	ds, err := zfs.GetDataset(fullID)
	if err != nil {
		return err
	}
	if ds.Type != "volume" {
		return ErrNotVolume
	}

	*response = rpc.VolumeResponse{
		Volumes: []*rpc.Volume{volumeFromDataset(ds)},
	}
	return nil
}

// DeleteDataset deletes a zfs dataset
func (store *ImageStore) DeleteDataset(r *http.Request, request *rpc.VolumeRequest, response *rpc.VolumeResponse) error {
	if request.ID == "" {
		return errors.New("need an id")
	}
	fullID := filepath.Join(store.config.Zpool, request.ID)
	ds, err := zfs.GetDataset(fullID)
	if err != nil {
		if isZfsNotFound(err) {
			return ErrNotFound
		}
		if isZfsInvalid(err) {
			return ErrNotValid
		}
		return err
	}

	if err := ds.Destroy(true); err != nil {
		return err
	}

	*response = rpc.VolumeResponse{
		Volumes: []*rpc.Volume{volumeFromDataset(ds)},
	}
	return nil
}
