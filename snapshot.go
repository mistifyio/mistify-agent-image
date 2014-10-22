package imagestore

import (
	"errors"
	"fmt"
	"github.com/mistifyio/go-zfs"
	"github.com/mistifyio/mistify-agent/rpc"
	"net/http"
	"path/filepath"
)

func snapshotFromDataset(ds *zfs.Dataset) *rpc.Snapshot {
	return &rpc.Snapshot{
		Id:   ds.Name,
		Size: ds.Volsize / 1024 / 1024, // what should this actually be?
	}
}

func (store *ImageStore) CreateSnapshot(r *http.Request, request *rpc.SnapshotRequest, response *rpc.SnapshotResponse) error {
	ds, err := zfs.GetDataset(request.Id)
	if err != nil {
		return err
	}
	if ds.Type == "snapshot" {
		return fmt.Errorf("cannot create a snapshot of a snapshot")
	}

	// TODO: validate input
	// TODO: allow options to be passed
	s, err := ds.Snapshot(request.Dest, default_zfs_options)

	if err != nil {
		return err
	}

	*response = rpc.SnapshotResponse{
		Snapshots: []*rpc.Snapshot{
			snapshotFromDataset(s),
		},
	}
	return nil
}

func (store *ImageStore) getSnapshot(id string) (*zfs.Dataset, error) {
	ds, err := zfs.GetDataset(id)
	if err != nil {
		return err
	}

	if ds.Type != "snapshot" {
		return NotSnapshot
	}

	return ds
}

func (store *ImageStore) DeleteSnapshot(r *http.Request, request *rpc.SnapshotRequest, response *rpc.SnapshotResponse) error {
	s, err := store.getSnapshot(request.Id)
	if err != nil {
		return err
	}
	// delete it
	return nil
}

func (store *ImageStore) GetSnapshot(r *http.Request, request *rpc.SnapshotRequest, response *rpc.SnapshotResponse) error {
	s, err := store.getSnapshot(request.Id)
	if err != nil {
		return err
	}
	*response = rpc.SnapshotResponse{
		Snapshots: []*rpc.Snapshot{
			snapshotFromDataset(s),
		},
	}
	return nil
}

func (store *ImageStore) ListSnapshots(r *http.Request, request *rpc.SnapshotRequest, response *rpc.SnapshotResponse) error {
	// see volumes for example?? we want to have a call/option to list snapshots for a particular ZFS entity??
	return nil
}
