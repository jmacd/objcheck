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
	"github.com/lightstep/lightstep-tracer-go"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
	"github.com/opentracing/opentracing-go/mocktracer"
)

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

type ObjCheckRequest struct {
	Service string `json:"service"`
	Region  string `json:"region"`
	Pool    int    `json:"pool"`
	Count   int    `json:"count"`
}

func (ocr ObjCheckRequest) validate() error {
	if ocr.Service != "gcs" {
		return errors.New("Bad service")
	}

	if ocr.Region != "us-central1" {
		return errors.New("Bad region")
	}

	if ocr.Pool != 10 {
		return errors.New("Bad pool")
	}

	if ocr.Count < 1 || ocr.Count > 1000 {
		return errors.New("Bad count")
	}

	return nil
}

func ObjCheck(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	span, ctx := opentracing.StartSpanFromContext(ctx, "ObjCheck")
	defer span.Finish()

	decoder := json.NewDecoder(r.Body)
	var ocr ObjCheckRequest
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

	for _, obj := range objList {
		requestObject(ctx, "objcheck-uscentral-1", obj)
	}

}

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

func requestObject(ctx context.Context, bucket string, object string) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "requestObject")
	defer span.Finish()

	client, err := storage.NewClient(ctx)
	if err != nil {
		fmt.Printf("client error: %s", err.Error())
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
		fmt.Printf("obj error: %s", err.Error())
		span.SetTag("error", true)
		span.LogFields(
			log.String("event", "obj error"),
			log.String("error", err.Error()),
		)
		return
	}

	defer rdr.Close()

	if _, err := io.Copy(ioutil.Discard, rdr); err != nil {
		fmt.Printf("io error: %s", err.Error())
		span.SetTag("error", true)
		span.LogFields(
			log.String("event", "io error"),
			log.String("error", err.Error()),
		)
		return
	}
}
