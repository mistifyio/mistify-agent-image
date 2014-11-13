package imagestore

import (
	"errors"
	"net/http"
	"strings"

	"github.com/mistifyio/kvite"
	"github.com/mistifyio/mistify-agent/rpc"
	"gopkg.in/mistifyio/go-zfs.v1"
)

type ()

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

// XXX: should these be serialized through a channel like clones are?

func (store *ImageStore) DeleteImage(r *http.Request, request *rpc.ImageRequest, response *rpc.ImageResponse) error {
	image, err := store.getImage(request.Id)
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
			return b.Delete(request.Id)
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

func (store *ImageStore) CloneImage(r *http.Request, request *rpc.ImageRequest, response *rpc.VolumeResponse) error {

	if request.Dest == "" {
		return errors.New("need dest")
	}

	image, err := store.getImage(request.Id)
	if err != nil {
		return err
	}

	snap, err := zfs.GetDataset(image.Snapshot)
	if err != nil {
		return err
	}

	clone, err := snap.Clone(request.Dest, default_zfs_options)
	if err != nil {
		return err
	}

	vol := volumeFromDataset(clone)

	*response = rpc.VolumeResponse{
		Volumes: []*rpc.Volume{vol},
	}
	return nil
}
