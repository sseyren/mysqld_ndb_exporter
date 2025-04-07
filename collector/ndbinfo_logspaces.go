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

// Scrape `ndbinfo.logspaces`.

package collector

import (
	"context"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

const ndbinfoLogSpacesQuery = `
	SELECT
		node_id,
		log_type,
		log_id,
		log_part,
		total,
		used
	FROM ndbinfo.logspaces;
`

// Metric descriptors.
var (
	ndbinfoLogSpacesDescTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "logspaces_bytes_total"),
		"Total space available for this log in bytes.",
		[]string{"node_id", "log_type", "log_id", "log_part"}, nil,
	)
	ndbinfoLogSpacesDescUsed = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "logspaces_bytes_used"),
		"Amount of space used by this log in bytes.",
		[]string{"node_id", "log_type", "log_id", "log_part"}, nil,
	)
)

// ScrapeNDBInfoLogSpaces collects from `ndbinfo.logspaces`.
type ScrapeNDBInfoLogSpaces struct{}

// Name of the Scraper. Should be unique.
func (ScrapeNDBInfoLogSpaces) Name() string {
	return ndbInfo + ".logspaces"
}

// Help describes the role of the Scraper.
func (ScrapeNDBInfoLogSpaces) Help() string {
	return "Collect metrics from ndbinfo.logspaces"
}

// Version of MySQL from which scraper is available.
func (ScrapeNDBInfoLogSpaces) Version() float64 {
	return 5.6
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeNDBInfoLogSpaces) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, ndbinfoLogSpacesQuery)
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
			ndbinfoLogSpacesDescTotal, prometheus.GaugeValue, float64(total),
			nodeID, logType, logID, logPart,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoLogSpacesDescUsed, prometheus.GaugeValue, float64(used),
			nodeID, logType, logID, logPart,
		)
	}

	return nil
}

// check interface
var _ Scraper = ScrapeNDBInfoLogSpaces{}
