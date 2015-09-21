package imagestore_test

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"testing"

	log "github.com/Sirupsen/logrus"
	"github.com/mistifyio/go-zfs"
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
	ImageService *httptest.Server
	ImageID      string
	ImageData    []byte
}

func (s *ImageTestSuite) SetupSuite() {
	s.APITestSuite.SetupSuite()

	s.ImageService = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != fmt.Sprintf("/images/%s/download", s.ImageID) {
			http.NotFound(w, r)
			return
		}
		if _, err := w.Write(s.ImageData); err != nil {
			log.WithField("error", err).Error("Failed to write mock image data to response")
		}
	}))

	imageURL, _ := url.Parse(s.ImageService.URL)

	s.StoreConfig.ImageServer = imageURL.Host
}

func (s *ImageTestSuite) SetupTest() {
	s.APITestSuite.SetupTest()

	if s.ImageID == "" {
		// Set up the image to be served from the test "image service" by
		// creating a volume, exporting a snapshot, and cleaning up. Only needs
		// to be done once, but can use an existing zpool if done in test setup.
		s.ImageID = uuid.New()
		volume, err := zfs.CreateVolume(filepath.Join(s.ID, s.ImageID), uint64(1*1024*1024), defaultZFSOptions)
		s.Require().NoError(err)
		snapshot, err := volume.Snapshot("test", false)
		s.Require().NoError(err)
		buff := new(bytes.Buffer)
		s.Require().NoError(snapshot.SendSnapshot(buff))
		s.ImageData = buff.Bytes()
		s.Require().NoError(volume.Destroy(zfs.DestroyRecursive))
	}
}

func TestImageTestSuite(t *testing.T) {
	suite.Run(t, new(ImageTestSuite))
}

func (s *ImageTestSuite) fetchImage() *rpc.Image {
	response := &rpc.ImageResponse{}
	request := &rpc.ImageRequest{
		ID: s.ImageID,
	}
	_ = s.Client.Do("ImageStore.RequestImage", request, response)
	return response.Images[0]
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
