package imagestore_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/mistifyio/go-zfs"
	"github.com/mistifyio/mistify-agent/rpc"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
)

type volumeTestCase struct {
	description string
	request     *rpc.VolumeRequest
	expectedErr bool
}

type VolumeTestSuite struct {
	APITestSuite
}

func TestVolumeTestSuite(t *testing.T) {
	suite.Run(t, new(VolumeTestSuite))
}

func (s *VolumeTestSuite) createVolume() (string, *rpc.Volume) {
	response := &rpc.VolumeResponse{}
	request := &rpc.VolumeRequest{
		ID:   uuid.New(),
		Size: 64,
	}
	_ = s.Client.Do("ImageStore.CreateVolume", request, response)

	// 10ms delay to prevent "dataset is busy" error
	time.Sleep(10 * time.Millisecond)

	return request.ID, response.Volumes[0]
}

func (s *VolumeTestSuite) runTestCases(method string, tests []*volumeTestCase, volume *rpc.Volume) {
	tests = append(tests, &volumeTestCase{"missing id should fail",
		&rpc.VolumeRequest{Size: 64}, true})

	for _, test := range tests {
		msg := testMsgFunc(test.description)
		response := &rpc.VolumeResponse{}
		err := s.Client.Do("ImageStore."+method, test.request, response)
		if test.expectedErr {
			s.Error(err, msg("should error"))
		} else {
			s.NoError(err, msg("should not error"))
			s.Len(response.Volumes, 1, msg("should return correct number of volumes"))
			if volume != nil {
				s.Equal(volume.ID, response.Volumes[0].ID, msg("should return expected volume"))
			}
		}
	}
}

func (s *VolumeTestSuite) TestList() {
	response := &rpc.VolumeResponse{}
	s.NoError(s.Client.Do("ImageStore.ListVolumes", &rpc.VolumeRequest{}, response))
	s.Len(response.Volumes, 0)

	_, volume := s.createVolume()
	response = &rpc.VolumeResponse{}
	s.NoError(s.Client.Do("ImageStore.ListVolumes", &rpc.VolumeRequest{}, response))
	s.Len(response.Volumes, 1)
	s.Equal(volume.ID, response.Volumes[0].ID)
}

func (s *VolumeTestSuite) TestCreate() {
	tests := []*volumeTestCase{
		{"missing size",
			&rpc.VolumeRequest{ID: "asdf"}, true},
		{"invalid size",
			&rpc.VolumeRequest{ID: "asdf", Size: 0}, true},
		{"valid request",
			&rpc.VolumeRequest{ID: uuid.New(), Size: 64}, false},
	}

	s.runTestCases("CreateVolume", tests, nil)
}

func (s *VolumeTestSuite) TestGet() {
	volumeName, volume := s.createVolume()

	fsName := "notAVolume"
	_, _ = zfs.CreateFilesystem(filepath.Join(s.ID, fsName), defaultZFSOptions)

	tests := []*volumeTestCase{
		{"non-existant volume",
			&rpc.VolumeRequest{ID: "adsf"}, true},
		{"request for non-volume",
			&rpc.VolumeRequest{ID: "fsName"}, true},
		{"valid request",
			&rpc.VolumeRequest{ID: volumeName}, false},
	}

	s.runTestCases("GetVolume", tests, volume)
}

func (s *VolumeTestSuite) TestDelete() {
	volumeName, volume := s.createVolume()

	tests := []*volumeTestCase{
		{"non-existant volume",
			&rpc.VolumeRequest{ID: "adsf"}, true},
		{"bad volume name",
			&rpc.VolumeRequest{ID: volumeName + "*"}, true},
		{"valid request",
			&rpc.VolumeRequest{ID: volumeName}, false},
	}

	s.runTestCases("DeleteDataset", tests, volume)
}
