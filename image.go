package imagestore

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/mistifyio/kvite"
	"github.com/mistifyio/mistify-agent/rpc"
	netutil "github.com/mistifyio/util/net"
	"gopkg.in/mistifyio/go-zfs.v1"
)

func isZfsNotFound(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "does not exist")
}

func isZfsInvalid(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "invalid dataset name")
}

// RequestImage fetches an image
func (store *ImageStore) RequestImage(r *http.Request, request *rpc.ImageRequest, response *rpc.ImageResponse) error {
	if request.ID == "" {
		return errors.New("need id")
	}

	// Check whether it exists locally
	image, err := store.getImage(request.ID)
	if err != nil && err != ErrNotFound {
		return err
	}

	// If it isn't here or ready, go get it
	if image == nil || image.Status != "complete" {
		hostport, err := netutil.HostWithPort(store.config.ImageServer)
		if err != nil {
			return err
		}
		req := &fetchRequest{
			name:    request.ID,
			source:  fmt.Sprintf("http://%s/images/%s/download", hostport, request.ID),
			tempdir: store.tempDir,
			dest:    filepath.Join(store.dataset, request.ID),
		}

		resp := store.fetcher.fetch(req)
		if resp.err != nil {
			return resp.err
		}

		// Get the image data
		image, err = store.getImage(request.ID)
		if err != nil {
			return err
		}
	}

	*response = rpc.ImageResponse{
		Images: []*rpc.Image{
			image,
		},
	}
	return nil
}

// ListImages lists the disk images
func (store *ImageStore) ListImages(r *http.Request, request *rpc.ImageRequest, response *rpc.ImageResponse) error {
	var images []*rpc.Image

	err := store.DB.Transaction(func(tx *kvite.Tx) error {
		if b, err := tx.Bucket("images"); b != nil {
			if err != nil {
				return err
			}
			err = b.ForEach(func(k string, v []byte) error {
				var i rpc.Image
				if err := json.Unmarshal(v, &i); err != nil {
					return err
				}
				images = append(images, &i)
				return nil
			})
			if err != nil {
				log.WithField("error", err).Error("failed to unmarshal image json")
				return err
			}
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

// GetImage gets a disk image
func (store *ImageStore) GetImage(r *http.Request, request *rpc.ImageRequest, response *rpc.ImageResponse) error {
	var images []*rpc.Image
	image, err := store.getImage(request.ID)
	if err != nil {
		return err
	} else {
		images = append(images, image)
	}

	// not found is an empty slice
	*response = rpc.ImageResponse{
		Images: images,
	}
	return nil
}

// DeleteImage deletes a disk image
func (store *ImageStore) DeleteImage(r *http.Request, request *rpc.ImageRequest, response *rpc.ImageResponse) error {
	image, err := store.getImage(request.ID)
	if err != nil {
		return err
	}
	for _, name := range []string{image.Snapshot, image.Volume} {
		if name != "" {
			d, err := zfs.GetDataset(name)

			if !isZfsNotFound(err) {
				if err != nil {
					return err
				}
				if err := d.Destroy(false); err != nil {
					return err
				}
			}
		}
	}

	err = store.DB.Transaction(func(tx *kvite.Tx) error {
		if b, err := tx.Bucket("images"); b != nil {
			if err != nil {
				return err
			}
			return b.Delete(request.ID)
		}
		return nil
	})
	if err != nil {
		return err
	}

	*response = rpc.ImageResponse{
		Images: []*rpc.Image{image},
	}
	return nil
}

// CloneImage clones a disk image
func (store *ImageStore) CloneImage(r *http.Request, request *rpc.ImageRequest, response *rpc.VolumeResponse) error {

	if request.Dest == "" {
		return errors.New("need dest")
	}

	image, err := store.getImage(request.ID)
	if err != nil {
		return err
	}

	snap, err := zfs.GetDataset(image.Snapshot)
	if err != nil {
		return err
	}

	clone, err := snap.Clone(request.Dest, defaultZFSOptions)
	if err != nil {
		return err
	}

	vol := volumeFromDataset(clone)

	*response = rpc.VolumeResponse{
		Volumes: []*rpc.Volume{vol},
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
	if image.ID == "" {
		return nil, ErrNotFound
	}
	return &image, nil
}

func (store *ImageStore) saveImage(image *rpc.Image) error {
	val, err := json.Marshal(image)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
			"func":  "json.Marshal",
		}).Error("failed to marshal image")
		return err
	}

	err = store.DB.Transaction(func(tx *kvite.Tx) error {
		b, err := tx.CreateBucketIfNotExists("images")
		if err != nil {
			return err
		}
		return b.Put(image.ID, val)
	})
	if err != nil {
		log.WithField("error", err).Error("failed to save image data")
	}
	return err
}
