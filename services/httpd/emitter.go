package httpd

import (
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
)

type Emitter struct {
	MaxRowLimit int
	Epoch       string
}

func (e *Emitter) Emit(w ResponseWriter, results <-chan *influxql.ResultSet) error {
	var convertToEpoch func(row *influxql.Row)
	if e.Epoch != "" {
		convertToEpoch = epochConverter(e.Epoch)
	}

	resp := Response{Results: make([]*influxql.Result, 0)}

	rows := 0
RESULTS:
	for result := range results {
		r := &influxql.Result{
			StatementID: result.ID,
			Messages:    result.Messages,
			Err:         result.Err,
		}
		resp.Results = append(resp.Results, r)
		if r.Err != nil {
			continue
		}

		for series := range result.SeriesCh() {
			if series.Err != nil {
				r.Err = series.Err
				continue RESULTS
			}

			s := &models.Row{
				Name:    series.Name,
				Tags:    series.Tags.KeyValues(),
				Columns: series.Columns,
			}
			r.Series = append(r.Series, s)

			for row := range series.RowCh() {
				if row.Err != nil {
					// TODO: Better implementation.
					r.Err = row.Err
					r.Series = nil
					continue RESULTS
				}

				if convertToEpoch != nil {
					convertToEpoch(&row)
				}
				s.Values = append(s.Values, row.Values)

				if e.MaxRowLimit > 0 && rows+len(s.Values) >= e.MaxRowLimit {
					s.Partial = true
					break RESULTS
				}
			}
		}
	}
	w.WriteResponse(resp)
	return nil
}

type ChunkedEmitter struct {
	ChunkSize int
	Epoch     string
}

func (e *ChunkedEmitter) Emit(w ResponseWriter, results <-chan *influxql.ResultSet) error {
	var convertToEpoch func(row *influxql.Row)
	if e.Epoch != "" {
		convertToEpoch = epochConverter(e.Epoch)
	}

	for result := range results {
		messages := result.Messages

		series := <-result.SeriesCh()
		if series == nil {
			w.WriteResponse(Response{Results: []*influxql.Result{
				{
					StatementID: result.ID,
					Messages:    messages,
				},
			}})
			continue
		} else if series.Err != nil {
			// An error occurred while processing the result.
			w.WriteResponse(Response{Results: []*influxql.Result{
				{
					StatementID: result.ID,
					Messages:    messages,
					Err:         series.Err,
				},
			}})
			continue
		}

		for series != nil {
			var values [][]interface{}
			for row := range series.RowCh() {
				if convertToEpoch != nil {
					convertToEpoch(&row)
				}

				if e.ChunkSize > 0 && len(values) >= e.ChunkSize {
					r := &influxql.Result{
						StatementID: result.ID,
						Series: []*models.Row{{
							Name:    series.Name,
							Tags:    series.Tags.KeyValues(),
							Columns: series.Columns,
							Values:  values,
							Partial: true,
						}},
						Messages: messages,
						Partial:  true,
					}
					w.WriteResponse(Response{Results: []*influxql.Result{r}})
					messages = nil
					values = values[:0]
				}
				values = append(values, row.Values)
			}

			r := &influxql.Result{
				StatementID: result.ID,
				Series: []*models.Row{{
					Name:    series.Name,
					Tags:    series.Tags.KeyValues(),
					Columns: series.Columns,
					Values:  values,
				}},
				Messages: messages,
			}

			series = <-result.SeriesCh()
			if series != nil {
				r.Partial = true
			}
			w.WriteResponse(Response{Results: []*influxql.Result{r}})
		}
	}
	return nil
}

func epochConverter(epoch string) func(row *influxql.Row) {
	divisor := int64(1)

	switch epoch {
	case "u":
		divisor = int64(time.Microsecond)
	case "ms":
		divisor = int64(time.Millisecond)
	case "s":
		divisor = int64(time.Second)
	case "m":
		divisor = int64(time.Minute)
	case "h":
		divisor = int64(time.Hour)
	}
	return func(row *influxql.Row) {
		if ts, ok := row.Values[0].(time.Time); ok {
			row.Values[0] = ts.UnixNano() / divisor
		}
	}
}
