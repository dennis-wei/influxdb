package httpd_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/services/httpd"
)

func EmitTestResults(results chan *influxql.ResultSet) {
	seriesCh := make(chan *influxql.Series)
	result := &influxql.ResultSet{
		ID:       0,
		Columns:  []string{"time", "value"},
		SeriesCh: seriesCh,
	}
	results <- result

	rowCh := make(chan influxql.Row)
	series := &influxql.Series{
		Name: "cpu",
		Tags: influxql.NewTags(map[string]string{
			"host": "server01",
		}),
		RowCh: rowCh,
	}
	seriesCh <- series

	rowCh <- influxql.Row{Values: []interface{}{time.Unix(0, 0).UTC(), 2.0}}
	rowCh <- influxql.Row{Values: []interface{}{time.Unix(10, 0).UTC(), 5.0}}
	rowCh <- influxql.Row{Values: []interface{}{time.Unix(20, 0).UTC(), 7.0}}
	close(rowCh)

	rowCh = make(chan influxql.Row)
	series = &influxql.Series{
		Name: "cpu",
		Tags: influxql.NewTags(map[string]string{
			"host": "server02",
		}),
		RowCh: rowCh,
	}
	seriesCh <- series
	close(seriesCh)

	rowCh <- influxql.Row{Values: []interface{}{time.Unix(0, 0).UTC(), 8.0}}
	close(rowCh)

	seriesCh = make(chan *influxql.Series)
	result = &influxql.ResultSet{
		ID:       1,
		Columns:  []string{"name"},
		SeriesCh: seriesCh,
	}
	results <- result
	close(results)

	rowCh = make(chan influxql.Row)
	series = &influxql.Series{
		Name:  "databases",
		RowCh: rowCh,
	}
	seriesCh <- series
	close(seriesCh)

	rowCh <- influxql.Row{Values: []interface{}{"db0"}}
	rowCh <- influxql.Row{Values: []interface{}{"db1"}}
	close(rowCh)
}

func TestEmitter(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://127.0.0.1:8086/query", nil)
	req.Header.Set("Accept", "application/json")
	resp := httptest.NewRecorder()
	w := httpd.NewResponseWriter(resp, req)

	results := make(chan *influxql.ResultSet)
	go EmitTestResults(results)

	emitter := &httpd.Emitter{}
	emitter.Emit(w, results)

	if have, want := strings.TrimSpace(resp.Body.String()), `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","value"],"values":[["1970-01-01T00:00:00Z",2],["1970-01-01T00:00:10Z",5],["1970-01-01T00:00:20Z",7]]},{"name":"cpu","tags":{"host":"server02"},"columns":["time","value"],"values":[["1970-01-01T00:00:00Z",8]]}]},{"statement_id":1,"series":[{"name":"databases","columns":["name"],"values":[["db0"],["db1"]]}]}]}`; have != want {
		t.Errorf("mismatched output:\n\thave=%v\n\twant=%v\n", have, want)
	}
}

func TestChunkedEmitter(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://127.0.0.1:8086/query", nil)
	req.Header.Set("Accept", "application/json")
	resp := httptest.NewRecorder()
	w := httpd.NewResponseWriter(resp, req)

	results := make(chan *influxql.ResultSet)
	go EmitTestResults(results)

	emitter := &httpd.ChunkedEmitter{ChunkSize: 2}
	emitter.Emit(w, results)

	if have, want := strings.TrimSpace(resp.Body.String()), `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","value"],"values":[["1970-01-01T00:00:00Z",2],["1970-01-01T00:00:10Z",5]],"partial":true}],"partial":true}]}
{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","value"],"values":[["1970-01-01T00:00:20Z",7]]}],"partial":true}]}
{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"server02"},"columns":["time","value"],"values":[["1970-01-01T00:00:00Z",8]]}]}]}
{"results":[{"statement_id":1,"series":[{"name":"databases","columns":["name"],"values":[["db0"],["db1"]]}]}]}`; have != want {
		t.Errorf("mismatched output:\n\thave=%v\n\twant=%v\n", have, want)
	}
}
