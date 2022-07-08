package s3 // import "go.unistack.org/micro-store-s3/v3"

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/gabriel-vasile/mimetype"
	"github.com/minio/minio-go/v7"
	creds "github.com/minio/minio-go/v7/pkg/credentials"
	"go.unistack.org/micro/v3/store"
)

var keyRegex = regexp.MustCompile("[^a-zA-Z0-9]+")

type s3Store struct {
	client    *minio.Client
	opts      store.Options
	endpoint  string
	mopts     *minio.Options
	connected bool
}

func getBucket(ctx context.Context) string {
	if ctx != nil {
		if v, ok := ctx.Value(bucketKey{}).(string); ok && v != "" {
			return v
		}
	}
	return ""
}

// NewStore create new s3 store
func NewStore(opts ...store.Option) store.Store {
	options := store.NewOptions(opts...)
	return &s3Store{opts: options, mopts: &minio.Options{}}
}

func (s *s3Store) Connect(ctx context.Context) error {
	if s.connected {
		return nil
	}

	client, err := minio.New(s.endpoint, s.mopts)
	if err != nil {
		return fmt.Errorf("Error connecting to store: %w", err)
	}

	s.client = client
	s.connected = true

	return nil
}

func (s *s3Store) Disconnect(ctx context.Context) error {
	return nil
}

func (s *s3Store) Init(opts ...store.Option) error {
	for _, o := range opts {
		o(&s.opts)
	}

	var akey, skey string
	region := "us-east-1"
	endpoint := s.endpoint

	if s.opts.Context != nil {
		if v, ok := s.opts.Context.Value(accessKey{}).(string); ok && v != "" {
			akey = v
		}
		if v, ok := s.opts.Context.Value(secretKey{}).(string); ok && v != "" {
			skey = v
		}
		if v, ok := s.opts.Context.Value(endpointKey{}).(string); ok && v != "" {
			endpoint = v
			var secure bool

			if strings.HasPrefix(endpoint, "https://") || s.opts.TLSConfig != nil {
				secure = true
			}

			if u, err := url.Parse(endpoint); err == nil && u.Host != "" {
				endpoint = u.Host
			}

			ts, err := minio.DefaultTransport(secure)
			if err != nil {
				return fmt.Errorf("init error: %w", err)
			}
			if s.opts.TLSConfig != nil {
				ts.TLSClientConfig = s.opts.TLSConfig
			}

			s.mopts.Transport = ts
			s.mopts.Secure = secure
			s.endpoint = endpoint
		}
		if v, ok := s.opts.Context.Value(regionKey{}).(string); ok && v != "" {
			region = v
		}

	}

	if len(akey) > 0 && len(skey) > 0 {
		s.mopts.Creds = creds.NewStaticV2(akey, skey, "")
	}

	s.mopts.Region = region

	return nil
}

func (s *s3Store) Read(ctx context.Context, key string, val interface{}, opts ...store.ReadOption) error {
	options := store.NewReadOptions(opts...)

	if len(key) == 0 {
		return store.ErrInvalidKey
	}

	bucket := getBucket(options.Context)
	if bucket == "" {
		bucket = options.Namespace
	}

	key = keyRegex.ReplaceAllString(key, "-")
	if s.opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.opts.Timeout)
		defer cancel()
	}
	rsp, err := s.client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if verr, ok := err.(minio.ErrorResponse); ok && verr.StatusCode == http.StatusNotFound {
		return store.ErrNotFound
	} else if err != nil {
		return err
	}

	_, err = rsp.Stat()
	if verr, ok := err.(minio.ErrorResponse); ok && verr.StatusCode == http.StatusNotFound {
		return store.ErrNotFound
	} else if err != nil {
		return err
	}

	switch val.(type) {
	case io.ReadCloser:
		val = rsp
	}

	defer rsp.Close()
	buf, err := ioutil.ReadAll(rsp)
	if err != nil {
		return err
	}

	return s.opts.Codec.Unmarshal(buf, val)
}

func (s *s3Store) Write(ctx context.Context, key string, val interface{}, opts ...store.WriteOption) error {
	options := store.NewWriteOptions(opts...)
	var r io.Reader

	if len(key) == 0 {
		return store.ErrInvalidKey
	}

	switch v := val.(type) {
	case io.Reader:
		r = v
	case []byte:
		r = bytes.NewReader(v)
	default:
		buf, err := s.opts.Codec.Marshal(v)
		if err != nil {
			return err
		}
		r = bytes.NewReader(buf)
	}

	key = keyRegex.ReplaceAllString(key, "-")

	bucket := getBucket(options.Context)
	if bucket == "" {
		bucket = options.Namespace
	}

	mputopts := minio.PutObjectOptions{}
	writeSize := int64(-1)
	if options.Context != nil {
		if v, ok := options.Context.Value(contentTypeKey{}).(string); ok && v != "" {
			mputopts.ContentType = v
		}
		if v, ok := options.Context.Value(writeSizeKey{}).(int64); ok && v > 0 {
			writeSize = v
		}
	}

	// slow path
	if mputopts.ContentType == "" {
		w := bytes.NewBuffer(nil)
		tr := io.TeeReader(r, w)
		mime, _ := mimetype.DetectReader(tr)
		mputopts.ContentType = mime.String()
		r = io.MultiReader(w, r)
	}
	if s.opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.opts.Timeout)
		defer cancel()
	}
	if ok, err := s.client.BucketExists(ctx, bucket); err != nil {
		return err
	} else if !ok {
		opts := minio.MakeBucketOptions{Region: s.mopts.Region}
		if err := s.client.MakeBucket(ctx, bucket, opts); err != nil {
			return err
		}
	}

	_, err := s.client.PutObject(ctx, bucket, key, r, writeSize, mputopts)
	return err
}

func (s *s3Store) Delete(ctx context.Context, key string, opts ...store.DeleteOption) error {
	// validate the key
	if len(key) == 0 {
		return store.ErrInvalidKey
	}

	// make the key safe for use with s3
	key = keyRegex.ReplaceAllString(key, "-")

	options := store.NewDeleteOptions(opts...)

	bucket := getBucket(options.Context)
	if bucket == "" {
		bucket = options.Namespace
	}
	if s.opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.opts.Timeout)
		defer cancel()
	}
	if len(bucket) > 0 {
		return s.client.RemoveObject(
			ctx,                                   // context
			bucket,                                // bucket name
			filepath.Join(options.Namespace, key), // object name
			minio.RemoveObjectOptions{},           // options
		)
	}

	return s.client.RemoveObject(
		ctx,                         // context
		options.Namespace,           // bucket name
		key,                         // object name
		minio.RemoveObjectOptions{}, // options
	)
}

func (s *s3Store) Options() store.Options {
	return s.opts
}

func (s *s3Store) Exists(ctx context.Context, key string, opts ...store.ExistsOption) error {
	options := store.NewExistsOptions(opts...)

	bucket := getBucket(options.Context)
	if bucket == "" {
		bucket = options.Namespace
	}
	if s.opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.opts.Timeout)
		defer cancel()
	}
	_, err := s.client.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
	if verr, ok := err.(minio.ErrorResponse); ok && verr.StatusCode == http.StatusNotFound {
		return store.ErrNotFound
	} else if err != nil {
		return err
	}

	return nil
}

func (s *s3Store) List(ctx context.Context, opts ...store.ListOption) ([]string, error) {
	options := store.NewListOptions(opts...)

	bucket := getBucket(options.Context)
	if bucket == "" {
		bucket = options.Namespace
	}

	var recursive bool
	if options.Context != nil {
		if v, ok := options.Context.Value(listRecursiveKey{}).(bool); ok {
			recursive = v
		}
	}
	if s.opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.opts.Timeout)
		defer cancel()
	}
	var names []string
	for oinfo := range s.client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: options.Prefix, Recursive: recursive}) {
		names = append(names, oinfo.Key)
	}

	return names, nil
}

func (s *s3Store) String() string {
	return "s3"
}

func (s *s3Store) Name() string {
	return s.opts.Name
}
