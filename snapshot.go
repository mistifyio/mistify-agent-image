package imagestore

import (
	"errors"
	"github.com/mistifyio/go-zfs"
	"github.com/mistifyio/mistify-agent/rpc"
	"net/http"
	"regexp"
)

func snapshotFromDataset(ds *zfs.Dataset) *rpc.Snapshot {
	return &rpc.Snapshot{
		Id:   ds.Name,
		Size: ds.Volsize / 1024 / 1024, // what should this actually be?
	}
}

func (store *ImageStore) CreateSnapshot(r *http.Request, request *rpc.SnapshotRequest, response *rpc.SnapshotResponse) error {
	if request.Id == "" {
		return errors.New("need an id")
	}

	ds, err := zfs.GetDataset(request.Id)
	if err != nil {
		return err
	}
	if ds.Type == "snapshot" {
		return errors.New("cannot create a snapshot of a snapshot")
	}

	if request.Dest == "" {
		return errors.New("need a dest")
	}

	var validName = regexp.MustCompile(`^[a-zA-Z0-9_\-:\.]+$`)
	if !validName.MatchString(request.Dest) {
		return errors.New("invalid snapshot dest")
	}

	recursive := false
	if request.Recursive {
		recursive = request.Recursive
	}

	s, err := ds.Snapshot(request.Dest, recursive)
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
		return nil, err
	}

	if ds.Type != "snapshot" {
		return nil, NotSnapshot
	}

	return ds, nil
}

func (store *ImageStore) DeleteSnapshot(r *http.Request, request *rpc.SnapshotRequest, response *rpc.SnapshotResponse) error {
	if request.Id == "" {
		return errors.New("need an id")
	}

	s, err := store.getSnapshot(request.Id)
	if err != nil {
		return err
	}

	recursive := false
	if request.Recursive {
		recursive = request.Recursive
	}

	if err := s.Destroy(recursive); err != nil {
		return err
	}

	*response = rpc.SnapshotResponse{
		Snapshots: []*rpc.Snapshot{
			snapshotFromDataset(s),
		},
	}
	return nil
}

func (store *ImageStore) GetSnapshot(r *http.Request, request *rpc.SnapshotRequest, response *rpc.SnapshotResponse) error {
	if request.Id == "" {
		return errors.New("need an id")
	}

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
	filter := store.config.Zpool
	if request.Id != "" {
		filter = request.Id
	}

	datasets, err := zfs.Snapshots(filter)
	if err != nil {
		return err
	}

	snapshots := make([]*rpc.Snapshot, len(datasets))
	for i, _ := range datasets {
		snapshots[i] = snapshotFromDataset(datasets[i])
	}

	*response = rpc.SnapshotResponse{
		Snapshots: snapshots,
	}
	return nil
}
