// Copyright 2018 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Scrape `ndbinfo.logbuffers`.

package collector

import (
	"context"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

const ndbinfoLogBuffersQuery = `
	SELECT
		node_id,
		log_type,
		log_id,
		log_part,
		total,
		used
	FROM ndbinfo.logbuffers;
`

// Metric descriptors.
var (
	ndbinfoLogBuffersDescTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "logbuffers_bytes_total"),
		"Total space available for this log in bytes.",
		[]string{"node_id", "log_type", "log_id", "log_part"}, nil,
	)
	ndbinfoLogBuffersDescUsed = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "logbuffers_bytes_used"),
		"Amount of space used by this log in bytes.",
		[]string{"node_id", "log_type", "log_id", "log_part"}, nil,
	)
)

// ScrapeNDBInfoLogBuffers collects from `ndbinfo.logbuffers`.
type ScrapeNDBInfoLogBuffers struct{}

// Name of the Scraper. Should be unique.
func (ScrapeNDBInfoLogBuffers) Name() string {
	return ndbInfo + ".logbuffers"
}

// Help describes the role of the Scraper.
func (ScrapeNDBInfoLogBuffers) Help() string {
	return "Collect metrics from ndbinfo.logbuffers"
}

// Version of MySQL from which scraper is available.
func (ScrapeNDBInfoLogBuffers) Version() float64 {
	return 5.6
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeNDBInfoLogBuffers) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, ndbinfoLogBuffersQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		nodeID, logType, logID, logPart string
		total, used                     uint64
	)
	for rows.Next() {
		if err := rows.Scan(
			&nodeID, &logType, &logID, &logPart,
			&total, &used,
		); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			ndbinfoLogBuffersDescTotal, prometheus.GaugeValue, float64(total),
			nodeID, logType, logID, logPart,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoLogBuffersDescUsed, prometheus.GaugeValue, float64(used),
			nodeID, logType, logID, logPart,
		)
	}
	return nil

}

// check interface
var _ Scraper = ScrapeNDBInfoLogBuffers{}
