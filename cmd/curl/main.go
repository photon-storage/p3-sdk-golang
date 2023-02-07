package main

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/photon-storage/p3-sdk-go/p3"
)

func main() {
	methodf := flag.String("method", "", "PUT or GET")
	bucketf := flag.String("bucket", "", "bucket name")
	keyf := flag.String("key", "", "key name")
	dataf := flag.String("data", "", "file path to data file")
	accessKeyIDf := flag.String(
		"access_key_id",
		"",
		"P3 access key ID, default to read from env P3_ACCESS_KEY_ID",
	)
	accessKeySecretf := flag.String(
		"access_key_secret",
		"",
		"P3 access key secret, default to read from env P3_ACCESS_KEY_SECRET",
	)
	flag.Parse()

	if *accessKeyIDf == "" {
		*accessKeyIDf = os.Getenv("P3_ACCESS_KEY_ID")
	}
	if *accessKeySecretf == "" {
		*accessKeySecretf = os.Getenv("P3_ACCESS_KEY_SECRET")
	}

	switch *methodf {
	case "GET":
		r, err := http.NewRequest(
			http.MethodGet,
			"",
			nil,
		)
		if err != nil {
			fmt.Printf("Error building curl request: %v\n", err)
			return
		}

		r.Header.Set("x-p3-bucket", *bucketf)
		r.Header.Set("x-p3-unixtime", fmt.Sprintf("%v", time.Now().Unix()))
		p3.AddAuthHeader(
			r,
			*bucketf,
			*keyf,
			*accessKeyIDf,
			[]byte(*accessKeySecretf),
		)

		var headers []string
		for h, vals := range r.Header {
			for _, val := range vals {
				headers = append(headers, fmt.Sprintf("-H \"%v: %v\"", h, val))
			}
		}

		fmt.Printf("Generated curl cmd:\n")
		fmt.Printf(
			"curl -X GET http://p3.photon.storage:13000/gateway/v1/%v"+
				" %v",
			*keyf,
			strings.Join(headers, " "),
		)

	case "PUT":
		data, err := ioutil.ReadFile(*dataf)
		if err != nil {
			fmt.Printf("Error reading file %v: %v\n", *dataf, err)
			return
		}

		r, err := http.NewRequest(
			http.MethodPut,
			"",
			bytes.NewReader(data),
		)
		if err != nil {
			fmt.Printf("Error building curl request: %v\n", err)
			return
		}

		r.Header.Set("x-p3-bucket", *bucketf)
		h := md5.Sum(data)
		r.Header.Set("x-p3-content-md5", hex.EncodeToString(h[:]))
		r.Header.Set("x-p3-content-type", "application/octet-stream")
		r.Header.Set("Content-Type", "application/octet-stream")
		r.Header.Set("x-p3-unixtime", fmt.Sprintf("%v", time.Now().Unix()))
		p3.AddAuthHeader(
			r,
			*bucketf,
			*keyf,
			*accessKeyIDf,
			[]byte(*accessKeySecretf),
		)

		var headers []string
		for h, vals := range r.Header {
			for _, val := range vals {
				headers = append(headers, fmt.Sprintf("-H \"%v: %v\"", h, val))
			}
		}

		fmt.Printf("Generated curl cmd:\n")
		fmt.Printf(
			"curl -X PUT http://p3.photon.storage:13000/gateway/v1/%v"+
				" --data-binary \"@%v\""+
				" %v",
			*keyf,
			*dataf,
			strings.Join(headers, " "),
		)

	default:
		fmt.Printf("Unsupported method: %v\n", *methodf)
	}
}
