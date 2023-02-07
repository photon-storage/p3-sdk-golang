package p3

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/text/encoding/unicode"
)

var (
	ErrReqDateMissing = errors.New("request date missing")
	ErrReqDateTooOld  = errors.New("request date too old")
)

// API authentication scheme similar to AWS S3.
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/RESTAuthentication.html
func sign(r *http.Request, bucket, key string, sk []byte) ([]byte, error) {
	// Part 1: positional parameters
	cmd5 := r.Header.Get("x-p3-content-md5")
	if cmd5 == "" {
		cmd5 = r.Header.Get("Content-MD5")
	}
	ctype := r.Header.Get("x-p3-content-type")
	if ctype == "" {
		ctype = r.Header.Get("Content-Type")
	}

	var ts time.Time
	utime := r.Header.Get("x-p3-unixtime")
	if utime != "" {
		v, err := strconv.ParseInt(utime, 10, 64)
		if err != nil {
			return nil, err
		}
		ts = time.Unix(v, 0)
	}
	if ts.IsZero() {
		date := r.Header.Get("Date")
		if date != "" {
			var err error
			if ts, err = http.ParseTime(date); err != nil {
				return nil, err
			}
		}
	}
	if ts.IsZero() {
		return nil, ErrReqDateMissing
	}
	if ts.Before(time.Now().Add(-15 * time.Minute)) {
		return nil, ErrReqDateTooOld
	}

	// Part 2: canonicalized x-p3- headers
	var p3Keys []string
	for k := range r.Header {
		k = strings.ToLower(k)
		if strings.HasPrefix(k, "x-p3-") {
			p3Keys = append(p3Keys, k)
		}
	}
	sort.Strings(p3Keys)
	var headers []string
	for _, k := range p3Keys {
		vals := r.Header.Values(k)
		headers = append(headers, k+":"+strings.Join(vals, ","))
	}

	// Part 3: canonicalized URI
	parts := strings.Split(bucket+"/"+key, "/")
	var arr []string
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		arr = append(arr, part)
	}
	uri := "/" + strings.Join(arr, "/")

	enc := unicode.UTF8.NewEncoder()
	k, err := enc.Bytes(sk)
	if err != nil {
		return nil, err
	}

	bytesToSign, err := enc.Bytes([]byte(r.Method + "\n" +
		cmd5 + "\n" +
		ctype + "\n" +
		ts.UTC().Format(time.RFC3339) + "\n" +
		strings.Join(headers, "\n") + "\n" +
		uri))
	if err != nil {
		return nil, err
	}

	h := hmac.New(sha1.New, k)
	if _, err := h.Write(bytesToSign); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

func AddAuthHeader(
	r *http.Request,
	bucket string,
	key string,
	accessKey string,
	sk []byte,
) error {
	sig, err := sign(r, bucket, key, sk)
	if err != nil {
		return err
	}

	enc := base64.StdEncoding.EncodeToString(sig)
	r.Header.Set("Authorization", accessKey+":"+enc)
	return nil
}
