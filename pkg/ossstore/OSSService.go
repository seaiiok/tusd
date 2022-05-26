package ossstore

import (
	"context"
	"io"
	"strconv"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

type ossService struct {
	// object storage bucket
	bucket *oss.Bucket
}

// OSSAPI aliyun oss sdk API
type OSSAPI interface {
	// GetObject
	//
	// download object by a object key
	//
	// io.ReadCloser	reader instance for reading data from response.
	// It must be called close() after the usage and only valid when error is nil.
	//
	// error 	it's nil if no error, otherwise it's an error object.
	//
	ReadObject(ctx context.Context, objectKey string) (io.ReadCloser, error)

	// GetObjectSize
	//
	// get the object size by a object key
	// get size from metadata
	//
	// error 	it's nil if no error, otherwise it's an error object.
	//
	GetObjectSize(ctx context.Context, objectKey string) (int64, error)

	// DeleteObject
	//
	// remove the object by object keys
	//
	// error 	it's nil if no error, otherwise it's an error object.
	//
	DeleteObject(ctx context.Context, objectKey ...string) error

	// WriteObject
	// creates a new object and it will overwrite the original one if it exists already.
	//
	// objectKey	the object key in UTF-8 encoding. The length must be between 1 and 1023, and cannot start with "/" or "\".
	// reader 	io.Reader instance for reading the data for uploading
	//
	// error 	it's nil if no error, otherwise it's an error object.
	//
	WriteObject(ctx context.Context, objectKey string, r io.Reader, opts ...oss.Option) error

	// AppendObject
	//
	// uploads the data in the way of appending an existing or new object.
	//
	// reader	io.Reader. The read instance for reading the data to append.
	// offset	the start position to append.
	//
	// int64	the next append position, it's valid when error is nil.
	// error	it's nil if no error, otherwise it's an error object.
	//
	AppendObject(ctx context.Context, objectKey string, r io.Reader, offset int64, opts ...oss.Option) (int64, error)

	// SignURL
	//
	// signs the URL. Users could access the object directly with this URL without getting the AK.
	//
	// objectKey    the target object to sign.
	//
	// string    returns the signed URL, when error is nil.
	// error    it's nil if no error, otherwise it's an error object.
	//
	SignURL(ctx context.Context, objectKey string, expiredInSec int64) (string, error)
}

// NewOSSService aliyun OSS(Object Storage Service)
// https://www.aliyun.com/product/oss
func NewOSSService(endpoint, accessKeyId, accessKeySecret, bucketName string) (OSSAPI, error) {
	opts := []oss.ClientOption{
		oss.EnableCRC(false),
	}
	client, err := oss.New(endpoint, accessKeyId, accessKeySecret, opts...)
	if err != nil {
		return nil, err
	}

	// if the bucket does not existed, then create it.
	if _, err := client.GetBucketStat(bucketName); err != nil {
		if err := client.CreateBucket(bucketName); err != nil {
			return nil, err
		}
	}

	accConfig := oss.TransferAccConfiguration{
		Enabled: true,
	}
	if err := client.SetBucketTransferAcc(bucketName, accConfig); err != nil {
		return nil, err
	}

	bucket, err := client.Bucket(bucketName)
	if err != nil {
		return nil, err
	}

	service := &ossService{
		bucket: bucket,
	}

	return service, nil
}

func (s *ossService) ReadObject(ctx context.Context, objectKey string) (io.ReadCloser, error) {
	return s.bucket.GetObject(objectKey)
}

func (s *ossService) GetObjectSize(ctx context.Context, objectKey string) (int64, error) {
	meta, err := s.bucket.GetObjectDetailedMeta(objectKey)
	if err != nil {
		return 0, err
	}
	length, err := strconv.ParseInt(meta.Get("Content-Length"), 10, 64)
	if err != nil {
		return 0, err
	}

	return length, nil
}

func (s *ossService) DeleteObject(ctx context.Context, objectKey ...string) error {
	_, err := s.bucket.DeleteObjects(objectKey)
	if err != nil {
		return err
	}

	return nil
}

func (s *ossService) WriteObject(ctx context.Context, objectKey string, r io.Reader, opts ...oss.Option) error {
	return s.bucket.PutObject(objectKey, r, opts...)
}

func (s *ossService) AppendObject(ctx context.Context, objectKey string, r io.Reader, offset int64, opts ...oss.Option) (int64, error) {
	n, err := s.bucket.AppendObject(objectKey, r, offset, opts...)
	if err != nil {
		return 0, err
	}

	return n, nil
}

func (s *ossService) SignURL(ctx context.Context, objectKey string, expiredInSec int64) (string, error) {
	signURL, err := s.bucket.SignURL(objectKey, oss.HTTPGet, expiredInSec)
	if err != nil {
		return "", err
	}

	return signURL, nil
}
