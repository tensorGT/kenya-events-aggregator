package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/cladder/ke-events/internal/db"
	"github.com/cladder/ke-events/internal/resilience"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type MediaStore struct {
	mc        *minio.Client
	db        *db.Store
	bucket    string
	publicURL string
	http      *http.Client
}

func NewMediaStore(endpoint, accessKey, secretKey, bucket, publicURL string, store *db.Store) (*MediaStore, error) {
	mc, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if exists, err := mc.BucketExists(ctx, bucket); err == nil && !exists {
		_ = mc.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
	}
	return &MediaStore{
		mc:        mc,
		db:        store,
		bucket:    bucket,
		publicURL: strings.TrimRight(publicURL, "/"),
		http:      &http.Client{Timeout: 15 * time.Second},
	}, nil
}

// MirrorImage downloads an external poster URL, deduplicates by SHA-256, and stores it in MinIO.
func (m *MediaStore) MirrorImage(ctx context.Context, imageURL string) (string, error) {
	if strings.TrimSpace(imageURL) == "" {
		return "", nil
	}
	if err := resilience.ValidateExternalURL(imageURL); err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, imageURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; KenyaEventsAggregator/1.0)")

	resp, err := m.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("image fetch HTTP %d", resp.StatusCode)
	}

	// Cap image size at 8 MB
	data, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil || len(data) == 0 {
		return "", fmt.Errorf("read image failed: %v", err)
	}

	sum := sha256.Sum256(data)
	hashHex := hex.EncodeToString(sum[:])

	// Check content-addressable deduplication in PostgreSQL
	if existing, ok := m.db.GetMediaBySHA256(ctx, hashHex); ok {
		return existing, nil
	}

	ct := resp.Header.Get("Content-Type")
	if ct == "" {
		ct = http.DetectContentType(data)
	}
	ext := extFromContentType(ct, imageURL)
	objectKey := fmt.Sprintf("posters/%s/%s%s", hashHex[:2], hashHex, ext)

	_, err = m.mc.PutObject(ctx, m.bucket, objectKey, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
		ContentType:  ct,
		CacheControl: "public, max-age=31536000, immutable",
	})
	if err != nil {
		return "", err
	}

	storageURL := fmt.Sprintf("%s/%s/%s", m.publicURL, m.bucket, objectKey)
	_ = m.db.SaveMediaBlob(ctx, hashHex, imageURL, m.bucket, objectKey, storageURL, ct, int64(len(data)))
	return storageURL, nil
}

func extFromContentType(ct, rawURL string) string {
	ct = strings.ToLower(ct)
	switch {
	case strings.Contains(ct, "png"):
		return ".png"
	case strings.Contains(ct, "webp"):
		return ".webp"
	case strings.Contains(ct, "svg"):
		return ".svg"
	case strings.Contains(ct, "jpeg"), strings.Contains(ct, "jpg"):
		return ".jpg"
	}
	ext := path.Ext(rawURL)
	if len(ext) > 1 && len(ext) <= 5 {
		return ext
	}
	return ".jpg"
}
