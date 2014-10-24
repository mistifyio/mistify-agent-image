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

/*
CreateSnapshot creates a snapshot of a zfs dataset.
    Request params:
    id        string : Req : Id of the zfs dataset to snapshot
    dest      string : Req : Name of the snapshot
    recursive bool   :     : Recursively create snapshots of descendents
*/
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

	s, err := ds.Snapshot(request.Dest, request.Recursive)
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

/*
DeleteSnapshot deletes a snapshot.
    Request params:
    id        string : Req : Full name of the snapshot
    recursive bool   :     : Recursively delete descendent snapshots
*/
func (store *ImageStore) DeleteSnapshot(r *http.Request, request *rpc.SnapshotRequest, response *rpc.SnapshotResponse) error {
	if request.Id == "" {
		return errors.New("need an id")
	}

	s, err := store.getSnapshot(request.Id)
	if err != nil {
		return err
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

/*
GetSnapshot retrieves information about a snapshot.
    Request params:
    id        string : Req : Full name of the snapshot
*/
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

/*
ListSnapshots retrieves a list of all snapshots for a dataset.
    Request params:
    id        string :     : Dataset to list snapshots for
*/
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
	for i := range datasets {
		snapshots[i] = snapshotFromDataset(datasets[i])
	}

	*response = rpc.SnapshotResponse{
		Snapshots: snapshots,
	}
	return nil
}
