package imagestore_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/mistifyio/go-zfs"
	"github.com/mistifyio/mistify-agent/client"
	"github.com/mistifyio/mistify-agent/rpc"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
)

type StoreTestSuite struct {
	APITestSuite
}

func TestStoreTestSuite(t *testing.T) {
	suite.Run(t, new(StoreTestSuite))
}

func (s *StoreTestSuite) TestSpaceAvailible() {
	size, err := s.Store.SpaceAvailible()
	s.NoError(err)
	s.True(size > 0)

	volumePath := fmt.Sprintf("%s/guests/%s", s.ID, uuid.New())
	_, _ = zfs.CreateVolume(volumePath, 10*1024*1024, defaultZFSOptions)
	sizeAfter, err := s.Store.SpaceAvailible()
	s.NoError(err)
	s.True(size > sizeAfter)
}

func (s *StoreTestSuite) TestVerifyDisks() {
	s.fetchImage()

	tests := []struct {
		description string
		request     *rpc.GuestRequest
		expectedErr bool
	}{
		{"missing guest",
			&rpc.GuestRequest{}, true},
		{"missing guest id",
			&rpc.GuestRequest{Guest: &client.Guest{}}, true},
		{"missing guest disks",
			&rpc.GuestRequest{Guest: &client.Guest{ID: uuid.New()}}, true},
		{"invalid disk size",
			&rpc.GuestRequest{Guest: &client.Guest{ID: uuid.New(), Disks: []client.Disk{{}}}}, true},
		{"too much required space",
			&rpc.GuestRequest{Guest: &client.Guest{ID: uuid.New(), Disks: []client.Disk{{Size: uint64(1e10)}}}}, true},
		{"valid request with size",
			&rpc.GuestRequest{Guest: &client.Guest{ID: uuid.New(), Disks: []client.Disk{{Size: uint64(10)}}}}, false},
		{"invalid image id",
			&rpc.GuestRequest{Guest: &client.Guest{ID: uuid.New(), Disks: []client.Disk{{Image: "asdf"}}}}, true},
		{"valid request with image id",
			&rpc.GuestRequest{Guest: &client.Guest{ID: uuid.New(), Disks: []client.Disk{{Image: s.ImageID}}}}, false},
	}

	for _, test := range tests {
		msg := testMsgFunc(test.description)
		response := &rpc.GuestResponse{}
		err := s.Client.Do("ImageStore.VerifyDisks", test.request, response)
		if test.expectedErr {
			s.Error(err, msg("should error"))
		} else {
			s.NoError(err, msg("should not error"))
		}
	}
}

func (s *StoreTestSuite) TestCreateGuestDisks() {
	s.fetchImage()

	diskFromImageRequest := &rpc.GuestRequest{Guest: &client.Guest{ID: uuid.New(), Disks: []client.Disk{{Image: s.ImageID}}}}

	tests := []struct {
		description string
		request     *rpc.GuestRequest
		expectedErr bool
	}{
		{"missing guest",
			&rpc.GuestRequest{}, true},
		{"missing guest id",
			&rpc.GuestRequest{Guest: &client.Guest{}}, true},
		{"missing guest disks",
			&rpc.GuestRequest{Guest: &client.Guest{ID: uuid.New()}}, true},
		{"invalid disk size",
			&rpc.GuestRequest{Guest: &client.Guest{ID: uuid.New(), Disks: []client.Disk{{}}}}, true},
		{"too much required space",
			&rpc.GuestRequest{Guest: &client.Guest{ID: uuid.New(), Disks: []client.Disk{{Size: uint64(math.Pow10(10))}}}}, true},
		{"valid request with size",
			&rpc.GuestRequest{Guest: &client.Guest{ID: uuid.New(), Disks: []client.Disk{{Size: uint64(10)}}}}, false},
		{"invalid image id",
			&rpc.GuestRequest{Guest: &client.Guest{ID: uuid.New(), Disks: []client.Disk{{Image: "asdf"}}}}, true},
		{"valid request with image id",
			diskFromImageRequest, false},
		{"repeat request with image id",
			diskFromImageRequest, false},
	}

	for _, test := range tests {
		msg := testMsgFunc(test.description)
		response := &rpc.GuestResponse{}
		err := s.Client.Do("ImageStore.CreateGuestDisks", test.request, response)
		if test.expectedErr {
			s.Error(err, msg("should error"))
		} else {
			s.NoError(err, msg("should not error"))
			for _, d := range response.Guest.Disks {
				s.NotEmpty(d.Source, msg("disk should have a source"))
			}
		}
	}
}

func (s *StoreTestSuite) TestDeleteGuestDisks() {
	s.fetchImage()

	guestID := uuid.New()
	request := &rpc.GuestRequest{Guest: &client.Guest{ID: guestID, Disks: []client.Disk{{Image: s.ImageID}}}}
	response := &rpc.GuestResponse{}
	s.NoError(s.Client.Do("ImageStore.CreateGuestDisks", request, response))

	tests := []struct {
		description string
		request     *rpc.GuestRequest
		expectedErr bool
	}{
		{"missing guest",
			&rpc.GuestRequest{}, true},
		{"missing guest id",
			&rpc.GuestRequest{Guest: &client.Guest{}}, true},
		{"guest id without disks",
			&rpc.GuestRequest{Guest: &client.Guest{ID: uuid.New()}}, true},
		{"guest id with disks",
			&rpc.GuestRequest{Guest: &client.Guest{ID: guestID}}, false},
	}

	for _, test := range tests {
		msg := testMsgFunc(test.description)
		response := &rpc.GuestResponse{}
		err := s.Client.Do("ImageStore.DeleteGuestsDisks", test.request, response)
		if test.expectedErr {
			s.Error(err, msg("should error"))
		} else {
			s.NoError(err, msg("should not error"))
			s.Len(response.Guest.Disks, 0, msg("should have no disks"))
		}
	}
}

func testMsgFunc(prefix string) func(...interface{}) string {
	return func(val ...interface{}) string {
		if len(val) == 0 {
			return prefix
		}
		msgPrefix := prefix + " : "
		if len(val) == 1 {
			return msgPrefix + val[0].(string)
		} else {
			return msgPrefix + fmt.Sprintf(val[0].(string), val[1:]...)
		}
	}
}
