package saastrace

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/storage"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/lightstep/lightstep-tracer-go"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
	"github.com/opentracing/opentracing-go/mocktracer"
)

// init on Cloud Function startup, initializes the LightStep tracer from environment variables
// or uses OpenTracing mocktracer
func init() {
	token := os.Getenv("LS_API_KEY")

	if token == "" {
		fmt.Println("Token from environment failed using mocktracer")
		tracer := mocktracer.New()
		opentracing.SetGlobalTracer(tracer)
	} else {
		tracer := lightstep.NewTracer(lightstep.Options{
			AccessToken: token,
			Tags:        opentracing.Tags{"region": os.Getenv("FUNCTION_REGION")},
		})
		opentracing.SetGlobalTracer(tracer)
	}

	fmt.Println("init() done")
}

var services = map[string]bool{
	"gcs": true,
	"s3":  true,
}

// bucketRegions map contains supported bucket region names
var bucketRegions = map[string]string{
	"us-central1":  "gcs",
	"us-east1":     "gcs",
	"europe-west2": "gcs",
	"us-east-2":    "s3",
}

// objCheckRequest holds cloud storage performance check parameters
type objCheckRequest struct {
	Service string `json:"service"`
	Region  string `json:"region"`
	Pool    int    `json:"pool"`
	Count   int    `json:"count"`
}

// Validate validates the requested check for service, region, pool, and count of objects to request
func (ocr objCheckRequest) validate() error {
	if !services[ocr.Service] {
		return fmt.Errorf("Bad service %v", ocr.Service)
	}

	if bucketRegions[ocr.Region] == "" {
		return fmt.Errorf("Bad region %v", ocr.Region)
	}

	if bucketRegions[ocr.Region] != ocr.Service {
		return fmt.Errorf("Bad service / region combination: %v and %v", ocr.Service, ocr.Region)
	}

	if ocr.Pool != 10 {
		return fmt.Errorf("Bad pool %v", ocr.Pool)
	}

	if ocr.Count < 1 || ocr.Count > 1000 {
		return errors.New("Bad count")
	}

	return nil
}

// ObjCheck measures the latency to fetch objects from pools in different regions in Google Cloud Storage
// triggered by HTTP requests to the deployed Google Cloud Function endpoint
func ObjCheck(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	span, ctx := opentracing.StartSpanFromContext(ctx, "ObjCheck")
	defer span.Finish()

	decoder := json.NewDecoder(r.Body)
	var ocr objCheckRequest
	err := decoder.Decode(&ocr)
	if err != nil {
		span.SetTag("error", true)
		span.LogEvent(err.Error())
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Write([]byte("Data Error"))
		return
	}

	err = ocr.validate()
	if err != nil {
		span.SetTag("error", true)
		span.LogEvent(err.Error())
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Write([]byte("Request Error"))
		return
	}

	objList, err := createObjList(ctx, ocr.Pool, ocr.Count, "1k")
	if err != nil {
		span.SetTag("error", true)
		span.LogEvent(err.Error())
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Write([]byte("List Error"))
		return
	}

	bucket := fmt.Sprintf("objcheck-%v", ocr.Region)

	for idx, obj := range objList {
		requestObject(ctx, ocr.Service, ocr.Region, bucket, obj, idx)
	}

}

// createObjList creates a list of random object keys given a pool and a number of objects to fetch
func createObjList(ctx context.Context, poolSize int, count int, size string) ([]string, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "createObjList")
	defer span.Finish()

	var objects []string

	if poolSize <= 0 {
		span.SetTag("error", true)
		span.LogEvent("Bad pool size")
		return objects, errors.New("Bad pool size")
	}
	rand.Seed(time.Now().UnixNano())
	min := 1
	max := poolSize
	for i := 0; i < count; i++ {
		objectID := rand.Intn(max-min) + min
		objects = append(objects, fmt.Sprintf("%v_%v_%v.obj", poolSize, objectID, size))
	}

	return objects, nil
}

// requestObject uses the Google Cloud Storage SDK to read an object from a bucket
// It reads all the data for the object but throws aways the actual contents
func requestObject(ctx context.Context, service string, region string, bucket string, object string, idx int) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "requestObject")
	defer span.Finish()

	span.SetTag("service", service)
	span.SetTag("bucket", bucket)
	span.SetTag("object", object)
	span.SetTag("seq", idx)

	if service == "gcs" {
		client, err := storage.NewClient(ctx)
		if err != nil {
			fmt.Printf("client error: %v\n", err.Error())
			span.SetTag("error", true)
			span.LogFields(
				log.String("event", "client error"),
				log.String("error", err.Error()),
			)
			return
		}

		bkt := client.Bucket(bucket)

		obj := bkt.Object(object)

		rdr, err := obj.NewReader(ctx)
		if err != nil {
			fmt.Printf("obj error: %s for %v\n", err.Error(), object)
			span.SetTag("error", true)
			span.LogFields(
				log.String("event", "obj error"),
				log.String("error", err.Error()),
			)
			return
		}

		defer rdr.Close()

		if _, err := io.Copy(ioutil.Discard, rdr); err != nil {
			fmt.Printf("io error: %v for %v\n", err.Error(), object)
			span.SetTag("error", true)
			span.LogFields(
				log.String("event", "io error"),
				log.String("error", err.Error()),
			)
			return
		}
	} else if service == "s3" {
		sess := session.Must(session.NewSession())
		svc := s3.New(sess, &aws.Config{
			Region: aws.String(region),
		})
		result, err := svc.GetObjectWithContext(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(object),
		})
		if err != nil {
			fmt.Printf("obj error: %s for %v\n", err.Error(), object)
			span.SetTag("error", true)
			span.LogFields(
				log.String("event", "obj error"),
				log.String("error", err.Error()),
			)
			return
		}

		// Make sure to close the body when done with it for S3 GetObject APIs or
		// will leak connections.
		defer result.Body.Close()

		if _, err := io.Copy(ioutil.Discard, result.Body); err != nil {
			fmt.Printf("io error: %v for %v\n", err.Error(), object)
			span.SetTag("error", true)
			span.LogFields(
				log.String("event", "io error"),
				log.String("error", err.Error()),
			)
			return
		}
	}

}
