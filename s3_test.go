package s3

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/unistack-org/micro/v3"
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

	svc := micro.NewService(micro.Store(s))
	if err := svc.Init(); err != nil {
		t.Fatalf("service init failed: %v", err)
	}

	val := []byte("test")
	key := "key"

	if err := s.Write(ctx, key, val, WriteBucket("micro-store-s3"), ContentType("text/plain")); err != nil {
		t.Fatal(err)
	}
	val = nil

	if err := s.Exists(ctx, key, ExistsBucket("micro-store-s3")); err != nil {
		t.Fatal(err)
	}

	if err := s.Read(ctx, key, &val, ReadBucket("micro-store-s3")); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(val, []byte("test")) {
		t.Fatalf("read bytes are not equal %s != %s", val, "test")
	}

	names, err := s.List(ctx, ListBucket("micro-store-s3"))
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

}
