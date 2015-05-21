package imagestore

import (
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/mistifyio/mistify-agent/rpc"
	"gopkg.in/mistifyio/go-zfs.v1"
)

var validName = regexp.MustCompile(`^[a-zA-Z0-9_\-:\.]+$`)

func snapshotFromDataset(ds *zfs.Dataset) *rpc.Snapshot {
	return &rpc.Snapshot{
		Id:   ds.Name,
		Size: ds.Written / 1024 / 1024,
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

	fullID := filepath.Join(store.config.Zpool, request.Id)
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
	fullID := filepath.Join(store.config.Zpool, id)
	ds, err := zfs.GetDataset(fullID)
	if err != nil {
		if isZfsNotFound(err) {
			return nil, ErrNotFound
		}
		if isZfsInvalid(err) {
			return nil, ErrNotValid
		}
		return nil, err
	}

	if ds.Type != "snapshot" {
		return nil, ErrNotSnapshot
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
		if isZfsNotFound(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	results := make([]*zfs.Dataset, 0, len(datasets))

	snapName := splitID[1]
	for i := range datasets {
		parts := strings.Split(datasets[i].Name, "@")
		if len(parts) == 2 && snapName == parts[1] {
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
	fullID := filepath.Join(store.config.Zpool, request.Id)
	datasets, err := zfs.Snapshots(fullID)
	if err != nil {
		if isZfsNotFound(err) {
			return ErrNotFound
		}
		return err
	}

	*response = rpc.SnapshotResponse{
		Snapshots: snapshotsFromDatasets(datasets),
	}
	return nil
}

// RollbackSnapshot performs a zfs snapshot rollback
func (store *ImageStore) RollbackSnapshot(r *http.Request, request *rpc.SnapshotRequest, response *rpc.SnapshotResponse) error {
	if request.Id == "" {
		return errors.New("need an id")
	}

	s, err := store.getSnapshot(request.Id)
	if err != nil {
		return err
	}

	if err = s.Rollback(request.DestroyMoreRecent); err != nil {
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
DownloadSnapshot downloads a zfs snapshot as a stream of data
    Request params:
    id        string : Req : Full name of the snapshot
*/
func (store *ImageStore) DownloadSnapshot(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var request rpc.SnapshotRequest
	err := decoder.Decode(&request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if request.Id == "" {
		http.Error(w, "need an id", http.StatusBadRequest)
		return
	}

	s, err := store.getSnapshot(request.Id)
	if err != nil {
		if err == ErrNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		if err == ErrNotSnapshot || err == ErrNotValid {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	err = s.SendSnapshot(w)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
