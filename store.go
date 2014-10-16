package imagestore

// TODO:
// - Add a job to cleanup any failed download attempts
// - Add a job to compare disk to metadata, remove any metadata that has no dataset - the opposite is harder

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mistifyio/kvite"
	"github.com/mistifyio/go-zfs"
	"github.com/mistifyio/mistify-agent/rpc"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
)

const (
	EAGAIN = syscall.EAGAIN
	EEXIST = syscall.EEXIST
	ENOSPC = syscall.ENOSPC
	EINVAL = syscall.EINVAL

	DBTABLE = "mistify-agent-image"
)

var (
	NotFound  = errors.New("not found")
	NotVolume = errors.New("not a volume")
)

type (

	// horrible hack
	Jobs struct {
		sync.RWMutex
		Requests map[string]*fetchRequest
	}

	ImageStore struct {
		// Config passed in
		config Config
		// clone worker - we only use one for now
		cloneWorker *cloneWorker
		// clone requests
		usersCloneChan chan *cloneRequest
		// fetch workers
		fetchWorkers []*fetchWorker
		// fetch requests
		fetcherChan chan *fetchRequest
		// fetch requests from "users"
		usersFetcherChan chan *fetchRequest
		// exit signal
		timeToDie chan struct{}
		// root of the image store
		dataset              string
		currentFetchRequests map[string]*fetchRequest
		DB                   *kvite.DB
		tempDir              string
		Jobs                 *Jobs
	}

	Config struct {
		ImageServer string // if we get a relative url, we prepend this
		NumFetchers uint   // workers to use for fetching images
		MaxPending  uint   // maximum number of queued fetch image
		Zpool       string
	}
)

func createJobs() *Jobs {
	return &Jobs{Requests: make(map[string]*fetchRequest)}
}

func (j *Jobs) Set(key string, val *fetchRequest) {
	j.Lock()
	defer j.Unlock()
	j.Requests[key] = val
}

func (j *Jobs) Get(key string) *fetchRequest {
	j.RLock()
	defer j.RUnlock()
	return j.Requests[key]
}

func (j *Jobs) Delete(key string) {
	j.Lock()
	defer j.Unlock()
	delete(j.Requests, key)
}

//create an image store with the given config
func Create(config Config) (*ImageStore, error) {
	if config.NumFetchers == 0 {
		config.NumFetchers = uint(runtime.NumCPU())
	}

	store := &ImageStore{
		config:           config,
		usersCloneChan:   make(chan *cloneRequest),
		fetcherChan:      make(chan *fetchRequest, config.MaxPending),
		usersFetcherChan: make(chan *fetchRequest),
		timeToDie:        make(chan struct{}),
		tempDir:          filepath.Join("/", config.Zpool, "images", "temp"),
		dataset:          filepath.Join(config.Zpool, "images"),
		Jobs:             createJobs(),
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

	//db, err := bolt.Open(filepath.Join("/", config.Zpool, "images", ".images.bolt"), 0644, nil)
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

	// start our fetchers
	store.fetchWorkers = make([]*fetchWorker, config.NumFetchers)
	for i := uint(0); i < config.NumFetchers; i++ {
		f := newFetchWorker(store, store.fetcherChan)
		store.fetchWorkers[i] = f
	}

	return store, nil
}

// destroy a store
func (store *ImageStore) Destroy() error {
	var q struct{}
	store.timeToDie <- q
	return nil
}

func (store *ImageStore) handleFetchResponse(request *fetchRequest) {
	store.fetcherChan <- request
	response := <-request.response

	fmt.Printf("handleFetchResponse: %+v\n", response)
	// should we record some type of status/error?

	store.Jobs.Delete(request.name)

	if response.err != nil {
		// log??
		return
	}
	image := rpc.Image{
		Id:       request.name,
		Volume:   response.dataset.Name,
		Snapshot: response.snapshot.Name,
		Size:     response.snapshot.Volsize / 1024 / 1024,
		Status:   "complete",
	}

	val, err := json.Marshal(image)
	if err != nil {
		// log?? destroy dataset?? set an error??
		return
	}
	// what if we get an error??
	store.DB.Transaction(func(tx *kvite.Tx) error {
		b, err := tx.CreateBucketIfNotExists("images")
		if err != nil {
			// log?? destroy dataset??
			return err
		}
		return b.Put(request.name, val)
	})
}

func (store *ImageStore) handleFetchRequest(req *fetchRequest) {
	// is someone else fetching this?

	if store.Jobs.Get(req.name) != nil {
		// return right away
		req.response <- &fetchResponse{}
		return
	}

	// Does it already exist?
	err := store.DB.Transaction(func(tx *kvite.Tx) error {
		b, err := tx.Bucket("images")
		if err != nil {
			return err
		}
		if b == nil {
			return NotFound
		}
		v, err := b.Get(req.name)
		if err != nil {
			return err
		}
		if v == nil {
			return NotFound
		}
		return nil
	})
	switch err {
	case NotFound:
		// okay, we need to fetch
	case nil:
		// it already exists
		req.response <- &fetchResponse{}
		return
	default:
		// some other error
		req.response <- &fetchResponse{err: err}
		return
	}

	request := &fetchRequest{}
	*request = *req

	request.response = make(chan *fetchResponse)

	fmt.Printf("handleFetchRequest: %+v\n", req)

	store.Jobs.Set(req.name, request)

	// set as pending
	image := rpc.Image{
		Id:     request.name,
		Status: "pending",
	}

	val, err := json.Marshal(image)
	if err != nil {
		// log?? destroy dataset?? set an error??
	} else {
		// what if we get an error??
		store.DB.Transaction(func(tx *kvite.Tx) error {
			b, err := tx.CreateBucketIfNotExists("images")
			if err != nil {
				// log?? destroy dataset??
				return err
			}
			return b.Put(request.name, val)
		})
	}
	fmt.Printf("handleFetchRequest: let the user know\n")

	// it's been queued
	req.response <- &fetchResponse{}

	//wait in a goroutine
	go store.handleFetchResponse(request)

}

func (store *ImageStore) Run() {
	for i, _ := range store.fetchWorkers {
		store.fetchWorkers[i].Run()
	}
	store.cloneWorker.Run()
	fmt.Println("I am here")
	for {
		select {
		case <-store.timeToDie:
			for _, f := range store.fetchWorkers {
				f.Exit()
			}
			store.cloneWorker.Exit()
			break

		case req := <-store.usersFetcherChan:
			store.handleFetchRequest(req)
		}
	}
}

func (store *ImageStore) RequestClone(name, dest string) (*zfs.Dataset, error) {

	fmt.Println("RequestClone: ", dest)

	i := &rpc.Image{}

	err := store.DB.Transaction(func(tx *kvite.Tx) error {
		b, err := tx.Bucket("images")
		if err != nil {
			return err
		}
		if b == nil {
			return NotFound
		}
		v, err := b.Get(name)
		if err != nil {
			return err
		}
		if v == nil {
			return NotFound
		}
		return json.Unmarshal(v, &i)
	})

	if err != nil {
		fmt.Println("RequestClone err", err)
		return nil, err
	}

	return store.cloneWorker.Clone(i.Snapshot, dest)
}

// asynchronously request an image
func (store *ImageStore) RequestImage(r *http.Request, request *rpc.ImageRequest, response *rpc.ImageResponse) error {
	if request.Source == "" {
		return errors.New("need source")
	}

	_, file := filepath.Split(request.Source)
	name := strings.TrimSuffix(file, ".gz")

	i := &rpc.Image{}

	err := store.DB.Transaction(func(tx *kvite.Tx) error {
		b, err := tx.Bucket("images")
		if err != nil {
			return err
		}
		if b == nil {
			return NotFound
		}
		v, err := b.Get(name)
		if err != nil {
			return err
		}
		if v == nil {
			return NotFound
		}
		return json.Unmarshal(v, &i)
	})

	switch err {
	case nil:
		// already exsists
		return EEXIST
	case NotFound:
		// need to fetch it
	default:
		return err
	}

	req := &fetchRequest{
		name:     name,
		source:   request.Source,
		tempdir:  store.tempDir,
		dest:     filepath.Join(store.dataset, name),
		response: make(chan *fetchResponse, 1),
	}

	fmt.Printf("RequestImage: %+v\n", req)

	store.usersFetcherChan <- req

	fmt.Printf("RequestImage: waiting on response\n")

	resp := <-req.response

	if resp.err != nil {
		return err
	}
	*response = rpc.ImageResponse{
		Images: []*rpc.Image{
			&rpc.Image{
				Id: name,
			},
		},
	}
	return nil
}

// TODO: have a background thread to update from datasets?  no images should come through
// unless they are in the database

func (store *ImageStore) ListImages(r *http.Request, request *rpc.ImageRequest, response *rpc.ImageResponse) error {
	images := make([]*rpc.Image, 0)

	err := store.DB.Transaction(func(tx *kvite.Tx) error {
		if b, err := tx.Bucket("images"); b != nil {
			if err != nil {
				return err
			}
			b.ForEach(func(k string, v []byte) error {
				var i rpc.Image
				if err := json.Unmarshal(v, &i); err != nil {
					return err
				}
				images = append(images, &i)
				return nil
			})
		}
		return nil
	})

	if err != nil {
		return err
	}
	*response = rpc.ImageResponse{
		Images: images,
	}
	return nil
}

func (store *ImageStore) getImage(id string) (*rpc.Image, error) {
	var image rpc.Image
	err := store.DB.Transaction(func(tx *kvite.Tx) error {
		if b, err := tx.Bucket("images"); b != nil {
			if err != nil {
				return err
			}
			v, err := b.Get(id)
			if err != nil {
				return err
			}
			if v == nil {
				return nil
			}
			return json.Unmarshal(v, &image)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if image.Id == "" {
		return nil, NotFound
	}
	return &image, nil
}

func (store *ImageStore) GetImage(r *http.Request, request *rpc.ImageRequest, response *rpc.ImageResponse) error {
	images := make([]*rpc.Image, 0)
	image, err := store.getImage(request.Id)
	if err != nil {
		if err != NotFound {
			return err
		}
	} else {
		images = append(images, image)
	}

	// not found is an empty slice
	*response = rpc.ImageResponse{
		Images: images,
	}
	return nil
}

// we are not "over-committing" on disk
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

//used for pre-flight check for vm creation
// we should also check to see if we have enough disk space for it. perhaps in a seperate call?
func (store *ImageStore) VerifyDisks(r *http.Request, request *rpc.GuestRequest, response *rpc.GuestResponse) error {
	if request.Guest == nil || request.Guest.Id == "" || len(request.Guest.Disks) == 0 {
		return EINVAL
	}
	availible, err := store.SpaceAvailible()
	if err != nil {
		return err
	}

	var total uint64

	for i, _ := range request.Guest.Disks {
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

	fmt.Printf("%d %d\n", total, availible)
	if total > availible {
		return ENOSPC
	}

	*response = rpc.GuestResponse{
		Guest: request.Guest,
	}
	return nil
}
