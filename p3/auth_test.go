package p3

import (
	"bytes"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/photon-storage/go-common/testing/require"
)

func TestSign(t *testing.T) {
	bucket := "/object_bucket"
	key := "/path/to/object/name"
	sk := []byte("mock_signing_key")

	cases := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "ok",
			run: func(t *testing.T) {
				r, err := http.NewRequest(
					"PUT",
					key,
					bytes.NewReader([]byte("object content")),
				)
				require.NoError(t, err)

				r.Header.Set("x-p3-content-md5", "md5_mock")
				r.Header.Set("x-p3-content-type", "application/octet-stream")
				r.Header.Set("x-p3-unixtime",
					fmt.Sprintf("%v", time.Now().Unix()),
				)
				r.Header.Set("x-p3-meta-1", "meta-1-val")
				r.Header.Set("x-p3-meta-2", "meta-2-val")

				sig, err := sign(r, bucket, key, sk)
				require.NoError(t, err)
				// HMAC-SHA1 returns 20-byte result
				require.Equal(t, 20, len(sig))
			},
		},
		{
			name: "missing content md5 and type ok",
			run: func(t *testing.T) {
				r, err := http.NewRequest(
					"PUT",
					key,
					bytes.NewReader([]byte("object content")),
				)
				require.NoError(t, err)

				r.Header.Set("x-p3-unixtime",
					fmt.Sprintf("%v", time.Now().Unix()),
				)
				r.Header.Set("x-p3-meta-1", "meta-1-val")
				r.Header.Set("x-p3-meta-2", "meta-2-val")

				sig, err := sign(r, bucket, key, sk)
				require.NoError(t, err)
				// HMAC-SHA1 returns 20-byte result
				require.Equal(t, 20, len(sig))
			},
		},
		{
			name: "missing timestamp",
			run: func(t *testing.T) {
				r, err := http.NewRequest(
					"PUT",
					key,
					bytes.NewReader([]byte("object content")),
				)
				require.NoError(t, err)

				r.Header.Set("x-p3-meta-1", "meta-1-val")
				r.Header.Set("x-p3-meta-2", "meta-2-val")

				_, err = sign(r, bucket, key, sk)
				require.ErrorIs(t, ErrReqDateMissing, err)
			},
		},
		{
			name: "timestamp too old",
			run: func(t *testing.T) {
				r, err := http.NewRequest(
					"PUT",
					key,
					bytes.NewReader([]byte("object content")),
				)
				require.NoError(t, err)

				r.Header.Set("x-p3-unixtime",
					fmt.Sprintf("%v", time.Now().Add(-20*time.Minute).Unix()),
				)
				r.Header.Set("x-p3-meta-1", "meta-1-val")
				r.Header.Set("x-p3-meta-2", "meta-2-val")

				_, err = sign(r, bucket, key, sk)
				require.ErrorIs(t, ErrReqDateTooOld, err)
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, c.run)
	}
}
