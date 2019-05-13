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
	"strings"
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

type SupportedCheckType int

const (
	UNSUPPORTED SupportedCheckType = iota + 1
	GCS
)

type CheckRequest struct {
	RequestType    string `json:"endpoint"`
	NormalizedType SupportedCheckType
	RequestTarget  string `json:"target"`
}

func (cr *CheckRequest) normalizeType() {
	rt := strings.ToLower(cr.RequestType)
	if rt == "gcs" {
		cr.NormalizedType = GCS
	} else {
		cr.NormalizedType = UNSUPPORTED
	}
}

func HTTPCheck(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var cr CheckRequest
	err := decoder.Decode(&cr)
	if err != nil {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Write([]byte("Data Error"))
		return
	}

	status := cr.doCheck()

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write([]byte(status))

}

func (cr CheckRequest) doCheck() string {
	cr.normalizeType()

	ctx := context.Background()

	rootSpan, ctx := opentracing.StartSpanFromContext(ctx, "doCheck")
	defer rootSpan.Finish()

	rootSpan.SetTag("type", cr.RequestType)
	rootSpan.SetTag("normtype", cr.NormalizedType)
	rootSpan.SetTag("target", cr.RequestTarget)

	var status string

	switch cr.NormalizedType {
	case UNSUPPORTED:
		status = "Unsupported Type"
	case GCS:
		status = cr.doGcsCheck(ctx)
	}

	return status
}

func (cr CheckRequest) doGcsCheck(ctx context.Context) string {
	span, ctx := opentracing.StartSpanFromContext(ctx, "doGcsCheck")
	defer span.Finish()

	client, err := storage.NewClient(ctx)
	if err != nil {
		fmt.Printf("client error: %s", err.Error())
		span.SetTag("error", true)
		span.LogFields(
			log.String("event", "client error"),
			log.String("error", err.Error()),
		)
		return "Check Error"
	}

	bkt := client.Bucket("ls-saastrace-mr")

	obj := bkt.Object(cr.RequestTarget)

	rdr, err := obj.NewReader(ctx)
	if err != nil {
		fmt.Printf("obj error: %s", err.Error())
		span.SetTag("error", true)
		span.LogFields(
			log.String("event", "obj error"),
			log.String("error", err.Error()),
		)
		return "Check Error"

	}
	defer rdr.Close()
	if _, err := io.Copy(ioutil.Discard, rdr); err != nil {
		fmt.Printf("io error: %s", err.Error())
		span.SetTag("error", true)
		span.LogFields(
			log.String("event", "io error"),
			log.String("error", err.Error()),
		)
		return "Check Error"
	}

	return "Check Success"
}

type ObjCheckRequest struct {
	Service string `json:"service"`
	Region  string `json:"region"`
	Pool    string `json:"pool"`
	Count   string `json:"count"`
}

func ObjCheck(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var ocr ObjCheckRequest
	err := decoder.Decode(&ocr)
	if err != nil {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Write([]byte("Data Error"))
		return
	}

}

func createObjList(ctx context.Context, poolSize int, count int, size string) ([]string, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "createObjList")
	defer span.Finish()

	var objects []string

	if poolSize <= 0 {
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
