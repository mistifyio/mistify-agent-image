package imagestore_test

import (
	"path/filepath"
	"testing"

	"github.com/mistifyio/mistify-agent/rpc"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
)

type imageTestCase struct {
	description string
	request     *rpc.ImageRequest
	expectedErr bool
}

type ImageTestSuite struct {
	APITestSuite
}

func TestImageTestSuite(t *testing.T) {
	suite.Run(t, new(ImageTestSuite))
}

func (s *ImageTestSuite) runTestCases(method string, tests []*imageTestCase) {
	basicTests := []*imageTestCase{
		{"missing id",
			&rpc.ImageRequest{}, true},
		{"non-existant id",
			&rpc.ImageRequest{ID: "asdf"}, true},
		{"valid id",
			&rpc.ImageRequest{ID: s.ImageID}, false},
	}

	tests = append(basicTests, tests...)

	for _, test := range tests {
		msg := func(val string) string {
			return test.description + " : " + val
		}
		response := &rpc.ImageResponse{}
		err := s.Client.Do("ImageStore."+method, test.request, response)
		images := response.Images
		if test.expectedErr {
			s.Error(err, msg("should error"))
			s.Len(images, 0, msg("bad request shoudln't return any images"))
		} else {
			s.NoError(err, msg("shouldn't error"))
			s.Len(images, 1, msg("should return the correct number of images"))
			s.Equal(s.ImageID, images[0].ID, msg("should return the correct image"))
			s.Equal("complete", images[0].Status, msg("should be a complete image"))
		}
	}
}

func (s *ImageTestSuite) TestRequestImage() {
	tests := []*imageTestCase{
		{"valid id already fetched",
			&rpc.ImageRequest{ID: s.ImageID}, false},
	}

	s.runTestCases("RequestImage", tests)
}

func (s *ImageTestSuite) TestListImages() {
	response := &rpc.ImageResponse{}
	request := &rpc.ImageRequest{}
	s.NoError(s.Client.Do("ImageStore.ListImages", request, response))
	s.Len(response.Images, 0)

	s.fetchImage()
	s.NoError(s.Client.Do("ImageStore.ListImages", request, response))
	s.Len(response.Images, 1)
}

func (s *ImageTestSuite) TestGetImage() {
	s.fetchImage()

	s.runTestCases("GetImage", nil)
}

func (s *ImageTestSuite) TestDeleteImage() {
	s.fetchImage()

	s.runTestCases("DeleteImage", nil)
}

// TODO: Sort out the clone functionality and then test it better
func (s *ImageTestSuite) TestCloneImage() {
	image := s.fetchImage()
	dest := filepath.Join(filepath.Dir(image.Volume), uuid.New())

	tests := []struct {
		description string
		request     *rpc.ImageRequest
		expectedErr bool
	}{
		{"missing id",
			&rpc.ImageRequest{Dest: dest}, true},
		{"missing dest",
			&rpc.ImageRequest{ID: "asdf"}, true},
		{"non-existant id",
			&rpc.ImageRequest{ID: "asdf", Dest: dest}, true},
		{"valid id",
			&rpc.ImageRequest{ID: s.ImageID, Dest: dest}, false},
	}

	for _, test := range tests {
		response := &rpc.VolumeResponse{}
		err := s.Client.Do("ImageStore.CloneImage", test.request, response)
		volumes := response.Volumes
		if test.expectedErr {
			s.Error(err)
			s.Len(volumes, 0)
		} else {
			s.NoError(err)
			s.Len(volumes, 1)
			s.Equal(dest, volumes[0].ID)
		}
	}
}

func (s *ImageTestSuite) TestRequestClone() {
	image := s.fetchImage()
	dest := filepath.Join(filepath.Dir(image.Volume), uuid.New())

	tests := []struct {
		description string
		name        string
		dest        string
		expectedErr bool
	}{
		{"missing id",
			"", dest, true},
		{"missing dest",
			"asdf", "", true},
		{"non-existant id",
			"asdf", dest, true},
		{"valid id",
			s.ImageID, dest, false},
	}

	for _, test := range tests {
		dataset, err := s.Store.RequestClone(test.name, test.dest)
		if test.expectedErr {
			s.Error(err)
			s.Nil(dataset)
		} else {
			s.NoError(err)
			s.Equal(dest, dataset.Name)
		}
	}
}
