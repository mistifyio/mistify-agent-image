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

	// Create a heirarchy of filesystems for snapshotting
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

// getID helps build various dataset ids with the correct format
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

// createSnapshot creates a snapshot of the parent dataset, optionally recursive
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
	id := s.getID(false, true, false, "")
	dest := uuid.New()

	tests := []struct {
		description string
		request     *rpc.SnapshotRequest
		expectedErr bool
	}{
		{"missing id",
			&rpc.SnapshotRequest{}, true},
		{"invalid id",
			&rpc.SnapshotRequest{ID: "+*?@#$"}, true},
		{"non-existant id",
			&rpc.SnapshotRequest{ID: "asdf"}, true},
		{"missing destination",
			&rpc.SnapshotRequest{ID: id}, true},
		{"invalid destination",
			&rpc.SnapshotRequest{ID: id, Dest: "-?_&"}, true},
		{"valid request",
			&rpc.SnapshotRequest{ID: id, Dest: dest}, false},
		{"duplicate request",
			&rpc.SnapshotRequest{ID: id, Dest: dest}, true},
		{"request to snapshot a snapshot",
			&rpc.SnapshotRequest{ID: s.getID(false, true, false, dest), Dest: dest}, true},
	}

	for _, test := range tests {
		msg := testMsgFunc(test.description)
		response := rpc.SnapshotResponse{}
		err := s.Client.Do("ImageStore.CreateSnapshot", test.request, response)
		if test.expectedErr {
			s.Error(err, msg("should error"))
		} else {
			s.NoError(err, msg("should not error"))
		}
	}
}

func (s *SnapshotTestSuite) TestCreateRecursive() {
	response := &rpc.SnapshotResponse{}
	request := &rpc.SnapshotRequest{
		ID:        s.getID(false, true, false, ""),
		Dest:      uuid.New(),
		Recursive: true,
	}
	s.NoError(s.Client.Do("ImageStore.CreateSnapshot", request, response))
	s.Len(response.Snapshots, 2)
}

func (s *SnapshotTestSuite) TestList() {
	tests := []struct {
		description  string
		request      *rpc.SnapshotRequest
		numSnapshots int
		expectedErr  bool
	}{
		{"list before snapshots",
			&rpc.SnapshotRequest{}, 0, false},
		{"list after snapshots",
			&rpc.SnapshotRequest{}, 2, false},
		{"list with id",
			&rpc.SnapshotRequest{ID: s.getID(false, true, true, "")}, 1, false},
		{"list with invalid id",
			&rpc.SnapshotRequest{ID: "asdf"}, 0, true},
	}

	for i, test := range tests {
		msg := testMsgFunc(test.description)
		response := &rpc.SnapshotResponse{}
		err := s.Client.Do("ImageStore.ListSnapshots", test.request, response)
		if test.expectedErr {
			s.Error(err, msg("should error"))
		} else {
			s.NoError(err, msg("should not error"))
		}
		s.Len(response.Snapshots, test.numSnapshots, msg("should return correct number of results"))

		// Create snapshots after the first empty list
		if i == 0 {
			// Create snapshots recursively, with one descendent
			_ = s.createSnapshot(true)
		}
	}
}

func (s *SnapshotTestSuite) TestGet() {
	snapshotName := s.createSnapshot(true)

	tests := []struct {
		description  string
		request      *rpc.SnapshotRequest
		numSnapshots int
		expectedErr  bool
	}{
		{"missing id",
			&rpc.SnapshotRequest{}, 0, true},
		{"invalid id",
			&rpc.SnapshotRequest{ID: "+*%$@"}, 0, true},
		{"non-existant id",
			&rpc.SnapshotRequest{ID: "asdf"}, 0, true},
		{"real id",
			&rpc.SnapshotRequest{ID: s.getID(false, true, false, snapshotName)}, 1, false},
		{"not a snapshot",
			&rpc.SnapshotRequest{ID: s.getID(false, true, false, "")}, 0, true},
	}

	for _, test := range tests {
		msg := testMsgFunc(test.description)
		response := &rpc.SnapshotResponse{}
		err := s.Client.Do("ImageStore.GetSnapshot", test.request, response)
		if test.expectedErr {
			s.Error(err, msg("should error"))
		} else {
			s.NoError(err, msg("should not error"))
		}
	}
}

func (s *SnapshotTestSuite) TestDelete() {
	snapshotName := s.createSnapshot(false)
	snapshotNameRecursive := s.createSnapshot(true)

	tests := []struct {
		description  string
		request      *rpc.SnapshotRequest
		numSnapshots int
		expectedErr  bool
	}{
		{"missing id",
			&rpc.SnapshotRequest{}, 0, true},
		{"invalid id",
			&rpc.SnapshotRequest{ID: "+*%$@"}, 0, true},
		{"non-existant id",
			&rpc.SnapshotRequest{ID: "asdf"}, 0, true},
		{"real id",
			&rpc.SnapshotRequest{ID: s.getID(false, true, false, snapshotName)}, 1, false},
		{"recursive with bad id",
			&rpc.SnapshotRequest{ID: s.getID(false, true, false, ""), Recursive: true}, 0, true},
		{"recursive id",
			&rpc.SnapshotRequest{ID: s.getID(false, true, false, snapshotNameRecursive), Recursive: true}, 2, false},
		{"not a snapshot",
			&rpc.SnapshotRequest{ID: s.getID(false, true, false, "")}, 0, true},
	}

	for _, test := range tests {
		msg := testMsgFunc(test.description)
		response := &rpc.SnapshotResponse{}
		err := s.Client.Do("ImageStore.DeleteSnapshot", test.request, response)
		if test.expectedErr {
			s.Error(err, msg("should error"))
		} else {
			s.NoError(err, msg("should not error"))
		}
	}
}

func (s *SnapshotTestSuite) TestDeleteRecursive() {
	snapshotName := s.createSnapshot(true)

	response := &rpc.SnapshotResponse{}
	request := &rpc.SnapshotRequest{
		ID:        s.getID(false, true, false, snapshotName),
		Recursive: true,
	}

	s.NoError(s.Client.Do("ImageStore.DeleteSnapshot", request, response))
	s.Len(response.Snapshots, 2)
}

func (s *SnapshotTestSuite) TestRollback() {
	snapshotName := s.createSnapshot(false)

	response := &rpc.SnapshotResponse{}
	request := &rpc.SnapshotRequest{
		ID: s.getID(false, true, false, snapshotName),
	}
	s.NoError(s.Client.Do("ImageStore.RollbackSnapshot", request, response))
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
	s.NoError(s.Client.Do("ImageStore.RollbackSnapshot", request, response))
	s.Len(response.Snapshots, 1)
}

func (s *SnapshotTestSuite) TestDownload() {
	snapshotName := s.createSnapshot(true)
	// special client for the non-rpc call
	client, _ := rpc.NewClient(uint(s.Port), "/snapshots/download")

	tests := []struct {
		description        string
		request            *rpc.SnapshotRequest
		expectedStatusCode int
	}{
		{"misisng request",
			nil, http.StatusBadRequest},
		{"missing id",
			&rpc.SnapshotRequest{}, http.StatusBadRequest},
		{"invalid id",
			&rpc.SnapshotRequest{ID: "+*%$@"}, http.StatusBadRequest},
		{"non-existant id",
			&rpc.SnapshotRequest{ID: "asdf"}, http.StatusNotFound},
		{"real id",
			&rpc.SnapshotRequest{ID: s.getID(false, true, false, snapshotName)}, http.StatusOK},
		{"not a snapshot",
			&rpc.SnapshotRequest{ID: s.getID(false, true, false, "")}, http.StatusBadRequest},
	}

	for _, test := range tests {
		msg := testMsgFunc(test.description)
		response := httptest.NewRecorder()
		client.DoRaw(test.request, response)
		s.Equal(test.expectedStatusCode, response.Code, msg("should return expected http status code"))
		if response.Code == http.StatusOK {
			s.True(len(response.Body.Bytes()) > 0, msg("should return snapshot data"))
		}
	}
}
