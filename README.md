# imagestore

[![imagestore](https://godoc.org/github.com/mistifyio/mistify-agent-image?status.png)](https://godoc.org/github.com/mistifyio/mistify-agent-image)

Package imagestore is a mistify subagent that manages guest disks and zfs,
exposed via JSON-RPC over HTTP.

### HTTP API Endpoints

    /_mistify_RPC_
    	* GET - Run a specified method

    /snapshots/download
    	* GET - Streaming download a zfs snapshot. Query with SnapshotRequest.

### Request Structure

    {
    	"method": "RPC_METHOD",
    	"params": [
    		DATA_STRUCT
    	],
    	"id": 0
    }

Where RPC_METHOD is the desired method and DATA_STRUCTURE is one of the request
structs defined in http://godoc.org/github.com/mistifyio/mistify-agent/rpc .

### Response Structure

    {
    	"result": {
    		KEY: RESPONSE_STRUCT
    	},
    	"error": null,
    	"id": 0
    }

Where KEY is a string (e.g. "snapshot") and DATA is one of the response structs
defined in http://godoc.org/github.com/mistifyio/mistify-agent/rpc .

### RPC Methods

    ListImages
    GetImage
    RequestImage
    DeleteImage
    CloneImage

    ListSnapshot
    GetSnapshot
    CreateSnapshot
    DeleteSnapshot
    RollbackSnapshot

    VerifyDisks
    CreateGuestDisks
    DeleteGuestDisks

See the godocs and function signatures for each method's purpose and expected
request/response structs.

## Usage

```go
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
```

```go
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
```

#### type Config

```go
type Config struct {
	ImageServer string // if we get a relative url, we prepend this
	NumFetchers uint   // workers to use for fetching images
	MaxPending  uint   // maximum number of queued fetch image
	Zpool       string
}
```

Config contains configuration for the ImageStore

#### type ImageStore

```go
type ImageStore struct {
	DB *kvite.DB

	Jobs *jobs
}
```

ImageStore manages disk images

#### func  Create

```go
func Create(config Config) (*ImageStore, error)
```
Create creates an image store with the given config

#### func (*ImageStore) CloneImage

```go
func (store *ImageStore) CloneImage(r *http.Request, request *rpc.ImageRequest, response *rpc.VolumeResponse) error
```
CloneImage clones a disk image

#### func (*ImageStore) CreateGuestDisks

```go
func (store *ImageStore) CreateGuestDisks(r *http.Request, request *rpc.GuestRequest, response *rpc.GuestResponse) error
```
CreateGuestDisks creates guest disks

#### func (*ImageStore) CreateSnapshot

```go
func (store *ImageStore) CreateSnapshot(r *http.Request, request *rpc.SnapshotRequest, response *rpc.SnapshotResponse) error
```
CreateSnapshot creates a snapshot of a zfs dataset.

    Request params:
    id        string : Req : Id of the zfs dataset to snapshot
    dest      string : Req : Name of the snapshot
    recursive bool   :     : Recursively create snapshots of descendents

#### func (*ImageStore) CreateVolume

```go
func (store *ImageStore) CreateVolume(r *http.Request, request *rpc.VolumeRequest, response *rpc.VolumeResponse) error
```
CreateVolume creates a zfs volume

#### func (*ImageStore) DeleteDataset

```go
func (store *ImageStore) DeleteDataset(r *http.Request, request *rpc.VolumeRequest, response *rpc.VolumeResponse) error
```
DeleteDataset deletes a zfs dataset

#### func (*ImageStore) DeleteGuestsDisks

```go
func (store *ImageStore) DeleteGuestsDisks(r *http.Request, request *rpc.GuestRequest, response *rpc.GuestResponse) error
```
DeleteGuestsDisks removes guests disks. It actually removes the entire guest
filesystem.

#### func (*ImageStore) DeleteImage

```go
func (store *ImageStore) DeleteImage(r *http.Request, request *rpc.ImageRequest, response *rpc.ImageResponse) error
```
DeleteImage deletes a disk image

#### func (*ImageStore) DeleteSnapshot

```go
func (store *ImageStore) DeleteSnapshot(r *http.Request, request *rpc.SnapshotRequest, response *rpc.SnapshotResponse) error
```
DeleteSnapshot deletes a snapshot.

    Request params:
    id        string : Req : Full name of the snapshot
    recursive bool   :     : Recursively delete descendent snapshots

#### func (*ImageStore) Destroy

```go
func (store *ImageStore) Destroy() error
```
Destroy destroys a store

#### func (*ImageStore) DownloadSnapshot

```go
func (store *ImageStore) DownloadSnapshot(w http.ResponseWriter, r *http.Request)
```
DownloadSnapshot downloads a zfs snapshot as a stream of data

    Request params:
    id        string : Req : Full name of the snapshot

#### func (*ImageStore) GetImage

```go
func (store *ImageStore) GetImage(r *http.Request, request *rpc.ImageRequest, response *rpc.ImageResponse) error
```
GetImage gets a disk image

#### func (*ImageStore) GetSnapshot

```go
func (store *ImageStore) GetSnapshot(r *http.Request, request *rpc.SnapshotRequest, response *rpc.SnapshotResponse) error
```
GetSnapshot retrieves information about a snapshot.

    Request params:
    id        string : Req : Full name of the snapshot

#### func (*ImageStore) GetVolume

```go
func (store *ImageStore) GetVolume(r *http.Request, request *rpc.VolumeRequest, response *rpc.VolumeResponse) error
```
GetVolume gets information about a zfs volume

#### func (*ImageStore) ListImages

```go
func (store *ImageStore) ListImages(r *http.Request, request *rpc.ImageRequest, response *rpc.ImageResponse) error
```
ListImages lists the disk images

#### func (*ImageStore) ListSnapshots

```go
func (store *ImageStore) ListSnapshots(r *http.Request, request *rpc.SnapshotRequest, response *rpc.SnapshotResponse) error
```
ListSnapshots retrieves a list of all snapshots for a dataset.

    Request params:
    id        string :     : Dataset to list snapshots for

#### func (*ImageStore) ListVolumes

```go
func (store *ImageStore) ListVolumes(r *http.Request, request *rpc.VolumeRequest, response *rpc.VolumeResponse) error
```
ListVolumes lists the zfs volumes

#### func (*ImageStore) RequestClone

```go
func (store *ImageStore) RequestClone(name, dest string) (*zfs.Dataset, error)
```
RequestClone clones a dataset

#### func (*ImageStore) RequestImage

```go
func (store *ImageStore) RequestImage(r *http.Request, request *rpc.ImageRequest, response *rpc.ImageResponse) error
```
RequestImage asynchronously requests an image

#### func (*ImageStore) RollbackSnapshot

```go
func (store *ImageStore) RollbackSnapshot(r *http.Request, request *rpc.SnapshotRequest, response *rpc.SnapshotResponse) error
```
RollbackSnapshot performs a zfs snapshot rollback

#### func (*ImageStore) Run

```go
func (store *ImageStore) Run()
```
Run starts processing for jobs

#### func (*ImageStore) RunHTTP

```go
func (store *ImageStore) RunHTTP(port uint) error
```
RunHTTP creates and runs the http server

#### func (*ImageStore) SpaceAvailible

```go
func (store *ImageStore) SpaceAvailible() (uint64, error)
```
SpaceAvailible returns the available disk space ensure we are not
"over-committing" on disk

#### func (*ImageStore) VerifyDisks

```go
func (store *ImageStore) VerifyDisks(r *http.Request, request *rpc.GuestRequest, response *rpc.GuestResponse) error
```
VerifyDisks verifys a guests's disk configuration before vm creation used for
pre-flight check for vm creation we should also check to see if we have enough
disk space for it. perhaps in a seperate call?

--
*Generated with [godocdown](https://github.com/robertkrimen/godocdown)*
