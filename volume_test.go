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
	return request.ID, response.Volumes[0]
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
	response := &rpc.VolumeResponse{}
	request := &rpc.VolumeRequest{}

	// Invalid size
	s.Error(s.Client.Do("ImageStore.CreateVolume", request, response), "need a valid size")

	// Missing ID
	request.Size = 64
	s.Error(s.Client.Do("ImageStore.CreateVolume", request, response), "need an id")

	// Good
	request.ID = uuid.New()
	s.NoError(s.Client.Do("ImageStore.CreateVolume", request, response), "should succeed")
}

func (s *VolumeTestSuite) TestGet() {
	volumeName, volume := s.createVolume()

	fsName := "notAVolume"
	_, _ = zfs.CreateFilesystem(filepath.Join(s.ID, fsName), defaultZFSOptions)

	response := &rpc.VolumeResponse{}
	request := &rpc.VolumeRequest{}

	// Missing ID
	s.Error(s.Client.Do("ImageStore.GetVolume", request, response), "need an id")

	// Not a volume
	request.ID = fsName
	s.Error(s.Client.Do("ImageStore.GetVolume", request, response), "should not get a non-volume")
	//helpers.Equals(t, imagestore.ErrNotVolume, err)

	request.ID = volumeName
	s.NoError(s.Client.Do("ImageStore.GetVolume", request, response), "should not get a non-volume")
	s.Len(response.Volumes, 1)
	s.Equal(volume.ID, response.Volumes[0].ID)
}

func (s *VolumeTestSuite) TestDelete() {
	volumeName, _ := s.createVolume()
	// 10ms delay to prevent "dataset is busy" error
	time.Sleep(10 * time.Millisecond)

	response := &rpc.VolumeResponse{}
	request := &rpc.VolumeRequest{}

	// Missing ID
	s.Error(s.Client.Do("ImageStore.DeleteDataset", request, response), "need an id")

	// Not found
	request.ID = "foobar"
	s.Error(s.Client.Do("ImageStore.DeleteDataset", request, response), "should not be found")

	// Invalid
	request.ID = volumeName + "*"
	s.Error(s.Client.Do("ImageStore.DeleteDataset", request, response), "invalid volume id")

	request.ID = volumeName
	s.NoError(s.Client.Do("ImageStore.DeleteDataset", request, response), "delete should succeed")
	s.Len(response.Volumes, 1)
}
