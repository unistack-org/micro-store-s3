package s3

import (
	"go.unistack.org/micro/v3/store"
)

type endpointKey struct{}

// Endpoint sets the endpoint option
func Endpoint(ep string) store.Option {
	return store.SetOption(endpointKey{}, ep)
}

type regionKey struct{}

// Region sets the region option
func Region(reg string) store.Option {
	return store.SetOption(regionKey{}, reg)
}

type accessKey struct{}

// AccessKey sets the AccessKey option
func AccessKey(akey string) store.Option {
	return store.SetOption(accessKey{}, akey)
}

type secretKey struct{}

// SecretKey sets the SecretKey option
func SecretKey(skey string) store.Option {
	return store.SetOption(secretKey{}, skey)
}

type bucketKey struct{}

// ReadBucket sets the bucket name option
func ReadBucket(bucket string) store.ReadOption {
	return store.SetReadOption(bucketKey{}, bucket)
}

// WriteBucket sets the bucket name option
func WriteBucket(bucket string) store.WriteOption {
	return store.SetWriteOption(bucketKey{}, bucket)
}

type writeSizeKey struct{}

// WriteSize key values size hint
func WriteSize(size int64) store.WriteOption {
	return store.SetWriteOption(writeSizeKey{}, size)
}

// DeleteBucket sets the bucket name option
func DeleteBucket(bucket string) store.DeleteOption {
	return store.SetDeleteOption(bucketKey{}, bucket)
}

type contentTypeKey struct{}

func ContentType(ct string) store.WriteOption {
	return store.SetWriteOption(contentTypeKey{}, ct)
}

// ListBucket sets the bucket name option
func ListBucket(bucket string) store.ListOption {
	return store.SetListOption(bucketKey{}, bucket)
}

type listRecursiveKey struct{}

// ListRecursive sets the bucket name option
func ListRecursive(b bool) store.ListOption {
	return store.SetListOption(listRecursiveKey{}, b)
}

// ExistsBucket sets the bucket name option
func ExistsBucket(bucket string) store.ExistsOption {
	return store.SetExistsOption(bucketKey{}, bucket)
}
