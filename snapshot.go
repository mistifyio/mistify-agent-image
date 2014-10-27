package imagestore

import (
	"errors"
	"net/http"
	"regexp"
	"strings"

	"github.com/mistifyio/go-zfs"
	"github.com/mistifyio/mistify-agent/rpc"
)

var validName = regexp.MustCompile(`^[a-zA-Z0-9_\-:\.]+$`)

func snapshotFromDataset(ds *zfs.Dataset) *rpc.Snapshot {
	return &rpc.Snapshot{
		Id:   ds.Name,
		Size: ds.Volsize / 1024 / 1024, // what should this actually be?
	}
}

func snapshotsFromDatasets(datasets []*zfs.Dataset) []*rpc.Snapshot {
	snapshots := make([]*rpc.Snapshot, len(datasets))
	for i, ds := range datasets {
		snapshots[i] = snapshotFromDataset(ds)
	}

	return snapshots
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

	if !validName.MatchString(request.Dest) {
		return errors.New("invalid snapshot dest")
	}

	s, err := ds.Snapshot(request.Dest, request.Recursive)
	if err != nil {
		return err
	}

	var snapshots []*rpc.Snapshot
	if request.Recursive {
		datasets, err := store.getSnapshotsRecursive(s.Name)
		if err != nil {
			return err
		}

		snapshots = snapshotsFromDatasets(datasets)
	} else {
		snapshots = []*rpc.Snapshot{
			snapshotFromDataset(s),
		}
	}

	*response = rpc.SnapshotResponse{
		Snapshots: snapshots,
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

func (store *ImageStore) getSnapshotsRecursive(id string) ([]*zfs.Dataset, error) {
	splitID := strings.Split(id, "@")
	if len(splitID) != 2 {
		return nil, errors.New("invalid snapshot name")
	}

	datasets, err := zfs.Snapshots(splitID[0])
	if err != nil {
		return nil, err
	}

	results := make([]*zfs.Dataset, 0, len(datasets))

	snapName := splitID[1]
	for i := range datasets {
		if snapName == strings.Split(datasets[i].Name, "@")[1] {
			results = append(results, datasets[i])
		}
	}

	return results, nil
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

	var snapshots []*rpc.Snapshot
	if request.Recursive {
		datasets, err := store.getSnapshotsRecursive(s.Name)
		if err != nil {
			return err
		}

		snapshots = snapshotsFromDatasets(datasets)
	} else {
		snapshots = []*rpc.Snapshot{
			snapshotFromDataset(s),
		}
	}

	if err := s.Destroy(request.Recursive); err != nil {
		return err
	}

	*response = rpc.SnapshotResponse{
		Snapshots: snapshots,
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

	*response = rpc.SnapshotResponse{
		Snapshots: snapshotsFromDatasets(datasets),
	}
	return nil
}
