package imagestore_test

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/mistifyio/go-zfs"
	imagestore "github.com/mistifyio/mistify-agent-image"
	rpc "github.com/mistifyio/mistify-agent/rpc"
	logx "github.com/mistifyio/mistify-logrus-ext"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/tylerb/graceful"
)

type APITestSuite struct {
	suite.Suite
	ID           string
	ZpoolDir     string
	Zpool        *zfs.Zpool
	Port         int
	StoreConfig  imagestore.Config
	Store        *imagestore.ImageStore
	Server       *graceful.Server
	Client       *rpc.Client
	ImageService *httptest.Server
	ImageID      string
	ImageData    []byte
}

func (s *APITestSuite) SetupSuite() {
	log.SetLevel(log.FatalLevel)
	s.Port = 54321
	s.Client, _ = rpc.NewClient(uint(s.Port), "")

	s.ImageService = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == fmt.Sprintf("/images/%s/download", s.ImageID) {
			if _, err := w.Write(s.ImageData); err != nil {
				log.WithField("error", err).Error("Failed to write mock image data to response")
			}
			return
		}

		if r.URL.Path == "/images/gzipID/download" {
			gzipWriter := gzip.NewWriter(w)
			defer gzipWriter.Close()
			if _, err := gzipWriter.Write(s.ImageData); err != nil {
				log.WithField("error", err).Error("Failed to write mock image data to response")
			}
			return
		}

		http.NotFound(w, r)
		return
	}))

	imageURL, _ := url.Parse(s.ImageService.URL)

	s.StoreConfig.ImageServer = imageURL.Host
}

func (s *APITestSuite) SetupTest() {
	var err error
	require := s.Require()

	// Create a zpool
	s.ID = "mist-" + uuid.New()
	s.StoreConfig.Zpool = s.ID
	s.ZpoolDir, err = ioutil.TempDir("", "APITestSuite-"+s.ID)
	require.NoError(err, "creating tempdir")
	zpoolFileNames := make([]string, 3)
	for i := range zpoolFileNames {
		file, err := ioutil.TempFile(s.ZpoolDir, "zfs-")
		require.NoError(err, "creating tempfile")
		defer logx.LogReturnedErr(file.Close, log.Fields{
			"filename": file.Name(),
		}, "failed to close tempfile")
		require.NoError(file.Truncate(int64(8*math.Pow10(7))), "truncate file") // 10MB file
		zpoolFileNames[i] = file.Name()
		defer logx.LogReturnedErr(func() error { return os.Remove(file.Name()) },
			log.Fields{"filename": file.Name()},
			"failed to remove tempfile")
	}
	s.Zpool, err = zfs.CreateZpool(s.ID, nil, zpoolFileNames...)
	require.NoError(err, "create zpool")

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

	// Run the ImageStore
	s.Store, err = imagestore.Create(s.StoreConfig)
	require.NoError(err)
	go s.Store.Run()
	s.Server = s.Store.RunHTTP(uint(s.Port))
}

func (s *APITestSuite) TearDownTest() {
	stopChan := s.Server.StopChan()
	s.Server.Stop(5 * time.Second)
	<-stopChan
	s.Store.Destroy()
	logx.LogReturnedErr(s.Zpool.Destroy, nil, "unable to destroy zpool "+s.ID)
	logx.LogReturnedErr(func() error { return os.RemoveAll(s.ZpoolDir) },
		nil, "unable to remove dir "+s.ZpoolDir)
}

func (s *APITestSuite) fetchImage() *rpc.Image {
	response := &rpc.ImageResponse{}
	request := &rpc.ImageRequest{
		ID: s.ImageID,
	}
	_ = s.Client.Do("ImageStore.RequestImage", request, response)
	return response.Images[0]
}

func init() {
	if _, err := zfs.ListZpools(); err != nil {
		log.WithField("error", err).Fatal("zfs error")
	}
}
