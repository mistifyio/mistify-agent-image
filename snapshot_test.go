package imagestore_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mistifyio/mistify-agent/rpc"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"gopkg.in/mistifyio/go-zfs.v1"
)

var defaultZFSOptions map[string]string = map[string]string{
	"compression": "lz4",
}

type SnapshotTestSuite struct {
	APITestSuite
	ParentFSName string
	ChildFSName  string
}

func (s *SnapshotTestSuite) SetupTest() {
	s.APITestSuite.SetupTest()

	s.ParentFSName = uuid.New()
	s.ChildFSName = uuid.New()

	// Create Parent
	_, err := zfs.CreateFilesystem(s.getID(true, true, false, ""), defaultZFSOptions)
	s.NoError(err)
	// Create Child
	_, err = zfs.CreateFilesystem(s.getID(true, true, true, ""), defaultZFSOptions)
	s.NoError(err)
}

func TestSnapshotTestSuite(t *testing.T) {
	suite.Run(t, new(SnapshotTestSuite))
}

func (s *SnapshotTestSuite) getID(zpool, parent, child bool, snapshotName string) string {
	pathParts := make([]string, 3)
	if zpool {
		pathParts[0] = s.ID
	}
	if parent {
		pathParts[1] = s.ParentFSName
	}
	if child {
		pathParts[2] = s.ChildFSName
	}

	// filepath.Join ignores empty strings
	path := filepath.Join(pathParts...)

	// strings.Join does not ignore empty strings, so can't just use it
	if snapshotName == "" {
		return path
	}
	return strings.Join([]string{path, snapshotName}, "@")
}

func (s *SnapshotTestSuite) createSnapshot(recursive bool) string {
	snapshotName := fmt.Sprintf("snap-%s", uuid.New())
	response := &rpc.SnapshotResponse{}
	request := &rpc.SnapshotRequest{
		ID:        s.getID(false, true, false, ""),
		Dest:      snapshotName,
		Recursive: recursive,
	}
	_ = s.Client.Do("ImageStore.CreateSnapshot", request, response)
	return snapshotName
}

func (s *SnapshotTestSuite) TestCreate() {
	response := &rpc.SnapshotResponse{}
	request := &rpc.SnapshotRequest{
		ID: s.getID(false, true, false, ""),
	}

	// No dest
	s.Error(s.Client.Do("ImageStore.CreateSnapshot", request, response), "dest should be required")

	// Invalid dest
	request.Dest = "-?_&"
	s.Error(s.Client.Do("ImageStore.CreateSnapshot", request, response), "dest should be invalid")

	// Successful
	request.Dest = uuid.New()
	s.NoError(s.Client.Do("ImageStore.CreateSnapshot", request, response), "snapshot should succeed")

	// Snapshot already exists
	s.Error(s.Client.Do("ImageStore.CreateSnapshot", request, response), "dataset should already exist")

	// Snapshot of a snapshot
	request.ID = s.getID(false, true, false, request.Dest)
	s.Error(s.Client.Do("ImageStore.CreateSnapshot", request, response), "should not be able to snapshot a snapshot")
}

func (s *SnapshotTestSuite) TestCreateRecursive() {
	response := &rpc.SnapshotResponse{}
	request := &rpc.SnapshotRequest{
		ID:        s.getID(false, true, false, ""),
		Dest:      uuid.New(),
		Recursive: true,
	}
	s.NoError(s.Client.Do("ImageStore.CreateSnapshot", request, response), "snapshot should succeed")
}

func (s *SnapshotTestSuite) TestList() {
	response := &rpc.SnapshotResponse{}

	// List on a clean setup
	request := &rpc.SnapshotRequest{}
	s.NoError(s.Client.Do("ImageStore.ListSnapshots", request, response), "list should succeed")
	snapshots := response.Snapshots
	s.Len(snapshots, 0)

	// Create snapshots recursively, with one descendent
	snapshotName := s.createSnapshot(true)

	// List from the top level
	request = &rpc.SnapshotRequest{}
	s.NoError(s.Client.Do("ImageStore.ListSnapshots", request, response), "list should succeed")
	snapshots = response.Snapshots
	s.Len(snapshots, 2)
	s.Equal(s.getID(true, true, false, snapshotName), snapshots[0].ID)
	s.Equal(s.getID(true, true, true, snapshotName), snapshots[1].ID)

	// List from the descendent
	request = &rpc.SnapshotRequest{
		ID: s.getID(false, true, true, ""),
	}
	s.NoError(s.Client.Do("ImageStore.ListSnapshots", request, response), "list should succeed")
	snapshots = response.Snapshots
	s.Len(snapshots, 1)
	s.Equal(s.getID(true, true, true, snapshotName), snapshots[0].ID)
}

func (s *SnapshotTestSuite) TestGet() {
	snapshotName := s.createSnapshot(true)

	response := &rpc.SnapshotResponse{}
	request := &rpc.SnapshotRequest{
		ID: s.getID(false, true, false, snapshotName),
	}

	s.NoError(s.Client.Do("ImageStore.GetSnapshot", request, response), "list should succeed")
	snapshots := response.Snapshots
	s.Len(snapshots, 1)
	s.Equal(s.getID(true, true, false, snapshotName), snapshots[0].ID)
}

func (s *SnapshotTestSuite) TestDelete() {
	snapshotName := s.createSnapshot(true)

	response := &rpc.SnapshotResponse{}
	request := &rpc.SnapshotRequest{
		ID: s.getID(false, true, false, snapshotName),
	}

	s.NoError(s.Client.Do("ImageStore.DeleteSnapshot", request, response), "list should succeed")
	s.Len(response.Snapshots, 1)
}

func (s *SnapshotTestSuite) TestDeleteRecursive() {
	snapshotName := s.createSnapshot(true)

	response := &rpc.SnapshotResponse{}
	request := &rpc.SnapshotRequest{
		ID:        s.getID(false, true, false, snapshotName),
		Recursive: true,
	}

	s.NoError(s.Client.Do("ImageStore.DeleteSnapshot", request, response), "list should succeed")
	s.Len(response.Snapshots, 2)
}

func (s *SnapshotTestSuite) TestRollback() {
	snapshotName := s.createSnapshot(false)

	response := &rpc.SnapshotResponse{}
	request := &rpc.SnapshotRequest{
		ID: s.getID(false, true, false, snapshotName),
	}
	s.NoError(s.Client.Do("ImageStore.RollbackSnapshot", request, response), "list should succeed")
	s.Len(response.Snapshots, 1)
}

func (s *SnapshotTestSuite) TestRollbackOlder() {
	snapshotName := s.createSnapshot(false)
	_ = s.createSnapshot(false)
	response := &rpc.SnapshotResponse{}
	request := &rpc.SnapshotRequest{
		ID:                s.getID(false, true, false, snapshotName),
		DestroyMoreRecent: true,
	}
	s.NoError(s.Client.Do("ImageStore.RollbackSnapshot", request, response), "list should succeed")
	s.Len(response.Snapshots, 1)
}

func (s *SnapshotTestSuite) TestDownload() {
	snapshotName := s.createSnapshot(false)
	// special client for the non-rpc call
	client, _ := rpc.NewClient(uint(s.Port), "/snapshots/download")
	request := &rpc.SnapshotRequest{}

	request.ID = ""
	resp := httptest.NewRecorder()
	client.DoRaw(request, resp)
	s.Equal(http.StatusBadRequest, resp.Code)

	request.ID = "asdf"
	resp = httptest.NewRecorder()
	client.DoRaw(request, resp)
	s.Equal(http.StatusNotFound, resp.Code)

	request.ID = s.getID(false, true, false, snapshotName)
	resp = httptest.NewRecorder()
	client.DoRaw(request, resp)
	s.Equal(http.StatusOK, resp.Code)
}
