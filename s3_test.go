package s3

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/unistack-org/micro/v3"
	"github.com/unistack-org/micro/v3/util/id"
)

func TestStore(t *testing.T) {
	if len(os.Getenv("INTEGRATION_TESTS")) > 0 {
		t.Skip()
	}

	ctx := context.Background()

	s := NewStore(
		AccessKey("Q3AM3UQ867SPQQA43P2F"),
		SecretKey("zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"),
		Endpoint("https://play.minio.io"),
	)

	if err := s.Init(); err != nil {
		t.Fatal(err)
	}

	if err := s.Init(); err != nil {
		t.Fatalf("double init test failed: %v", err)
	}

	if err := s.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	if err := s.Connect(ctx); err != nil {
		t.Fatalf("double connect test failed: %v", err)
	}

	defer func() {
		if err := s.Disconnect(ctx); err != nil {
			t.Fatal(err)
		}
	}()

	svc := micro.NewService(micro.Stores(s))
	if err := svc.Init(); err != nil {
		t.Fatalf("service init failed: %v", err)
	}

	val := []byte("test")
	key := "key"

	uid, err := id.New()
	if err != nil {
		t.Fatal(err)
	}
	bucket := "micro-store-s3-" + uid
	if err := s.Write(ctx, key, val, WriteBucket(bucket), ContentType("text/plain")); err != nil {
		t.Fatal(err)
	}
	val = nil

	if err := s.Exists(ctx, key, ExistsBucket(bucket)); err != nil {
		t.Fatal(err)
	}

	if err := s.Read(ctx, key, &val, ReadBucket(bucket)); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(val, []byte("test")) {
		t.Fatalf("read bytes are not equal %s != %s", val, "test")
	}

	names, err := s.List(ctx, ListBucket(bucket))
	if err != nil {
		t.Fatal(err)
	}

	var found bool
	for _, n := range names {
		if n == "key" {
			found = true
		}
	}

	if !found {
		t.Fatalf("key not found in %v", names)
	}

	objectsCh := make(chan minio.ObjectInfo)
	minioClient := s.(*s3Store).client
	// Send object names that are needed to be removed to objectsCh
	go func() {
		defer close(objectsCh)
		// List all objects from a bucket-name with a matching prefix.
		for object := range minioClient.ListObjects(context.Background(), bucket, minio.ListObjectsOptions{Recursive: true}) {
			if object.Err != nil {
				t.Fatal(object.Err)
			}
			objectsCh <- object
		}
	}()

	opts := minio.RemoveObjectsOptions{
		GovernanceBypass: true,
	}

	for rErr := range minioClient.RemoveObjects(context.Background(), bucket, objectsCh, opts) {
		t.Fatalf("Error detected during deletion: %v", rErr)
	}
}
