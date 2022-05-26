package ossstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/tus/tusd/pkg/handler"
)

type OSSStore struct {
	service OSSAPI
}

// aliyun OSS(Object Storage Service)
// https://www.aliyun.com/product/oss
func New(service OSSAPI) OSSStore {
	return OSSStore{
		service: service,
	}
}

func (store OSSStore) UseIn(composer *handler.StoreComposer) {
	composer.UseCore(store)
	composer.UseTerminater(store)
}

func (store OSSStore) NewUpload(ctx context.Context, info handler.FileInfo) (handler.Upload, error) {
	if filehash, ok := info.MetaData["filehash"]; ok {
		info.ID = filehash
	} else {
		return nil, errors.New("miss filehash")
	}

	info.Storage = map[string]string{
		"Type": "ossstore",
		"Key":  store.binPath(info.ID),
	}

	upload := &ossUpload{
		info:  info,
		store: &store,
	}

	data, _ := json.Marshal(info)
	r := bytes.NewReader(data)

	if err := store.service.WriteObject(ctx, store.infoPath(info.ID), r); err != nil {
		return nil, err
	}

	return upload, nil
}

func (store OSSStore) GetUpload(ctx context.Context, id string) (handler.Upload, error) {
	info := handler.FileInfo{}
	binPath := store.binPath(id)
	infoPath := store.infoPath(id)

	r, err := store.service.ReadObject(ctx, infoPath)
	if err != nil {
		return nil, err
	}

	buf := bytes.Buffer{}
	if _, err := buf.ReadFrom(r); err != nil {
		return nil, err
	}

	if err := json.Unmarshal(buf.Bytes(), &info); err != nil {
		return nil, err
	}

	offset, _ := store.service.GetObjectSize(ctx, binPath)

	info.Offset = offset

	return &ossUpload{
		info:  info,
		store: &store,
	}, nil
}

func (store OSSStore) AsTerminatableUpload(upload handler.Upload) handler.TerminatableUpload {
	return upload.(*ossUpload)
}

func (store OSSStore) AsLengthDeclarableUpload(upload handler.Upload) handler.LengthDeclarableUpload {
	return upload.(*ossUpload)
}

func (store OSSStore) AsConcatableUpload(upload handler.Upload) handler.ConcatableUpload {
	return upload.(*ossUpload)
}

func (store OSSStore) binPath(id string) string {
	return id
}

func (store OSSStore) infoPath(id string) string {
	return id + ".info"
}

type ossUpload struct {
	info handler.FileInfo

	store *OSSStore
}

func (upload ossUpload) GetInfo(ctx context.Context) (handler.FileInfo, error) {
	return upload.info, nil
}

func (upload ossUpload) WriteChunk(ctx context.Context, offset int64, src io.Reader) (int64, error) {
	id := upload.info.ID
	store := upload.store
	binPath := store.binPath(id)

	filename := ""
	if v, ok := upload.info.MetaData["filename"]; ok {
		filename = v
	}

	//if v, ok := upload.info.MetaData["filetype"]; ok {
	//	filetype = v
	//}

	contentDisposition := fmt.Sprintf("attachment; filename=%s", filename)
	n, err := store.service.AppendObject(ctx, binPath, src, offset, oss.ContentDisposition(contentDisposition))
	if err != nil {
		return 0, err
	}

	upload.info.Offset += n
	return n, err
}

func (upload ossUpload) GetReader(ctx context.Context) (io.Reader, error) {
	id := upload.info.ID
	store := upload.store
	binPath := store.binPath(id)

	return store.service.ReadObject(ctx, binPath)
}

func (upload ossUpload) Terminate(ctx context.Context) error {
	id := upload.info.ID
	store := upload.store
	infoPath := store.infoPath(id)
	binPath := store.binPath(id)

	err := store.service.DeleteObject(ctx, infoPath, binPath)
	if err != nil {
		return err
	}

	return nil
}

func (upload ossUpload) ConcatUploads(ctx context.Context, uploads []handler.Upload) error {
	return nil
}

func (upload ossUpload) DeclareLength(ctx context.Context, length int64) error {
	return nil
}

func (upload ossUpload) FinishUpload(ctx context.Context) error {
	return nil
}
