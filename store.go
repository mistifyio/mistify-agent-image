package imagestore

// TODO:
// - Add a job to cleanup any failed download attempts
// - Add a job to compare disk to metadata, remove any metadata that has no dataset - the opposite is harder

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"

	log "github.com/Sirupsen/logrus"
	"github.com/mistifyio/kvite"
	"github.com/mistifyio/mistify-agent/client"
	"github.com/mistifyio/mistify-agent/rpc"
	logx "github.com/mistifyio/mistify-logrus-ext"
	"gopkg.in/mistifyio/go-zfs.v1"
)

const (
	// EAGAIN is a shortcut to syscall.EAGAIN
	EAGAIN = syscall.EAGAIN
	// EEXIST is a shortcut to syscall.EEXIST
	EEXIST = syscall.EEXIST
	// ENOSPC is a shortcut to syscall.ENOSPC
	ENOSPC = syscall.ENOSPC
	// EINVAL is a shortcut to syscall.EINVAL
	EINVAL = syscall.EINVAL

	// DBTABLE is the tablename for images
	DBTABLE = "mistify-agent-image"
)

var (
	// ErrNotFound is an error when resource not being found
	ErrNotFound = errors.New("not found")
	// ErrNotVolume is an error when the resouce is expected to be a volume and isn't
	ErrNotVolume = errors.New("not a volume")
	// ErrNotSnapshot is an error when the resouce is expected to be a snapshot and isn't
	ErrNotSnapshot = errors.New("not a snapshot")
	// ErrNotValid is an error when the resouce is expected to be a dataset and isn't
	ErrNotValid = errors.New("not a valid dataset")
)

type (
	// ImageStore manages disk images
	ImageStore struct {
		// Config passed in
		config Config
		// clone worker - we only use one for now
		cloneWorker *cloneWorker
		// clone requests
		usersCloneChan chan *cloneRequest
		fetcher        *fetcher
		// exit signal
		timeToDie chan struct{}
		// root of the image store
		dataset string
		DB      *kvite.DB
		tempDir string
	}

	// Config contains configuration for the ImageStore
	Config struct {
		ImageServer string // if we get a relative url, we prepend this
		NumFetchers uint   // workers to use for fetching images
		MaxPending  uint   // maximum number of queued fetch image
		Zpool       string
	}
)

// Create creates an image store with the given config
func Create(config Config) (*ImageStore, error) {
	if config.NumFetchers == 0 {
		config.NumFetchers = uint(runtime.NumCPU())
	}

	store := &ImageStore{
		config:         config,
		usersCloneChan: make(chan *cloneRequest),
		timeToDie:      make(chan struct{}),
		tempDir:        filepath.Join("/", config.Zpool, "images", "temp"),
		dataset:        filepath.Join(config.Zpool, "images"),
	}

	_, err := zfs.GetDataset(store.dataset)
	if err != nil {
		if strings.Contains(err.Error(), "dataset does not exist") {
			_, err := zfs.CreateFilesystem(store.dataset, nil)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	fi, err := os.Stat(store.tempDir)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(store.tempDir, 0755); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	if fi != nil {
		if !fi.Mode().IsDir() {
			return nil, fmt.Errorf("%s is not a directory", store.tempDir)
		}
	}

	db, err := kvite.Open(filepath.Join("/", config.Zpool, "images", ".images.db"), DBTABLE)
	if err != nil {
		return nil, err
	}
	store.DB = db
	err = store.DB.Transaction(func(tx *kvite.Tx) error {
		_, err := tx.CreateBucketIfNotExists("images")
		return err
	})

	if err != nil {
		return nil, err
	}

	// start our clone worker
	store.cloneWorker = newCloneWorker(store)

	// start the fetcher
	store.fetcher = newFetcher(store, config.MaxPending, config.NumFetchers)

	return store, nil
}

// Destroy destroys a store
func (store *ImageStore) Destroy() error {
	var q struct{}
	// singal shutdown
	store.timeToDie <- q
	// wait for shutdown
	<-store.timeToDie
	return nil
}

// Run starts processing for jobs
func (store *ImageStore) Run() {
	store.cloneWorker.Run()
	store.fetcher.run()
	q := <-store.timeToDie
	store.cloneWorker.Exit()
	store.fetcher.exit()
	logx.LogReturnedErr(store.DB.Close, nil, "failed to close store")
	store.timeToDie <- q
}

// RequestClone clones a dataset
func (store *ImageStore) RequestClone(name, dest string) (*zfs.Dataset, error) {

	log.WithField("RequestClone", dest).Info()

	i := &rpc.Image{}

	err := store.DB.Transaction(func(tx *kvite.Tx) error {
		b, err := tx.Bucket("images")
		if err != nil {
			return err
		}
		if b == nil {
			return ErrNotFound
		}
		v, err := b.Get(name)
		if err != nil {
			return err
		}
		if v == nil {
			return ErrNotFound
		}
		return json.Unmarshal(v, &i)
	})

	if err != nil {
		return nil, err
	}

	return store.cloneWorker.Clone(i.Snapshot, dest)
}

// TODO: have a background thread to update from datasets?  no images should come through
// unless they are in the database

// SpaceAvailible returns the available disk space
// ensure we are not "over-committing" on disk
func (store *ImageStore) SpaceAvailible() (uint64, error) {
	var total uint64
	ds, err := zfs.GetDataset(store.config.Zpool)
	if err != nil {
		return 0, err
	}
	total = ds.Avail
	if ds.Quota != 0 && ds.Quota < total {
		total = ds.Quota
	}

	ds, err = zfs.GetDataset(filepath.Join(store.config.Zpool, "guests"))
	if err != nil {
		return 0, err
	}

	if ds.Quota != 0 && ds.Quota < total {
		total = ds.Quota
	}

	datasets, err := zfs.Datasets(store.config.Zpool)
	if err != nil {
		return 0, err
	}

	for _, ds := range datasets {
		switch ds.Type {
		//filesystems roll up into top-level usage (I think)
		case "filesystem":

		case "snapshot":
			// not sure this is correct
			total = total - ds.Written

		case "volume":
			total = total - ds.Volsize

		}
	}

	return total / 1024, nil
}

// VerifyDisks verifys a guests's disk configuration before vm creation
// used for pre-flight check for vm creation
// we should also check to see if we have enough disk space for it. perhaps in a seperate call?
func (store *ImageStore) VerifyDisks(r *http.Request, request *rpc.GuestRequest, response *rpc.GuestResponse) error {
	if request.Guest == nil || request.Guest.ID == "" || len(request.Guest.Disks) == 0 {
		return EINVAL
	}
	availible, err := store.SpaceAvailible()
	if err != nil {
		return err
	}

	var total uint64

	for i := range request.Guest.Disks {
		disk := &request.Guest.Disks[i]
		if disk.Image == "" && disk.Size == 0 {
			return EINVAL
		}
		if disk.Image != "" {
			image, err := store.getImage(disk.Image)
			if err != nil {
				return err
			}
			disk.Size = image.Size
		}
		total = total + disk.Size
	}

	if total > availible {
		return ENOSPC
	}

	*response = rpc.GuestResponse{
		Guest: request.Guest,
	}
	return nil
}

// CreateGuestDisks creates guest disks
func (store *ImageStore) CreateGuestDisks(r *http.Request, request *rpc.GuestRequest, response *rpc.GuestResponse) error {
	err := store.VerifyDisks(r, request, response)
	if err != nil {
		return err
	}
	// VerifyDisks filled in response
	guest := response.Guest

	for i := range guest.Disks {
		disk := &guest.Disks[i]

		disk.Volume = fmt.Sprintf("%s/guests/%s/disk-%d", store.config.Zpool, guest.ID, i)

		_, err := zfs.GetDataset(disk.Volume)

		if err == nil {
			//already exists
			continue
		} else {
			if !strings.Contains(err.Error(), "does not exist") {
				return err
			}
		}

		if disk.Image != "" {
			image, err := store.getImage(disk.Image)
			if err != nil {
				return err
			}
			s, err := zfs.GetDataset(image.Snapshot)
			if err != nil {
				return err
			}
			ds, err := s.Clone(disk.Volume, defaultZFSOptions)
			if err != nil {
				return err
			}
			disk.Source = deviceForDataset(ds)
		} else {
			ds, err := zfs.CreateVolume(disk.Volume, disk.Size*1024*1024, defaultZFSOptions)
			if err != nil {
				return err
			}
			disk.Source = deviceForDataset(ds)
		}
	}
	return nil
}

// DeleteGuestsDisks removes guests disks.  It actually removes the entire guest filesystem.
func (store *ImageStore) DeleteGuestsDisks(r *http.Request, request *rpc.GuestRequest, response *rpc.GuestResponse) error {
	if request.Guest == nil || request.Guest.ID == "" {
		return EINVAL
	}
	name := fmt.Sprintf("%s/guests/%s", store.config.Zpool, request.Guest.ID)

	ds, err := zfs.GetDataset(name)

	*response = rpc.GuestResponse{
		Guest: request.Guest,
	}
	response.Guest.Disks = []client.Disk{}

	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			// not there
			return nil
		}
		return err
	}

	// we assume guest disk were created by this service, or at least in the same structure
	if err := ds.Destroy(true); err != nil {
		return err
	}

	return nil
}
