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

// Scrape `ndbinfo.memoryusage`.

package collector

import (
	"context"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

const ndbinfoMemoryUsageQuery = `
	SELECT
		node_id,
		memory_type,
		used,
		total
	FROM ndbinfo.memoryusage;
`

// Metric descriptors.
var (
	ndbinfoMemoryUsageDescUsed = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "memoryusage_used"),
		"Number of bytes currently used for data memory or index memory by this data node.",
		[]string{"node_id", "memory_type"}, nil,
	)
	ndbinfoMemoryUsageDescTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "memoryusage_total"),
		"Total number of bytes of data memory or index memory available for this data node.",
		[]string{"node_id", "memory_type"}, nil,
	)
)

// ScrapeNDBInfoMemoryUsage collects from `ndbinfo.memoryusage`.
type ScrapeNDBInfoMemoryUsage struct{}

// Name of the Scraper. Should be unique.
func (ScrapeNDBInfoMemoryUsage) Name() string {
	return ndbInfo + ".memoryusage"
}

// Help describes the role of the Scraper.
func (ScrapeNDBInfoMemoryUsage) Help() string {
	return "Collect metrics from ndbinfo.memoryusage"
}

// Version of MySQL from which scraper is available.
func (ScrapeNDBInfoMemoryUsage) Version() float64 {
	return 5.6
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeNDBInfoMemoryUsage) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, ndbinfoMemoryUsageQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		nodeID, memoryType string
		used, total        uint64
	)
	for rows.Next() {
		if err := rows.Scan(
			&nodeID, &memoryType,
			&used, &total,
		); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			ndbinfoMemoryUsageDescUsed, prometheus.GaugeValue, float64(used),
			nodeID, memoryType,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoMemoryUsageDescTotal, prometheus.GaugeValue, float64(total),
			nodeID, memoryType,
		)
	}

	return nil
}

// check interface
var _ Scraper = ScrapeNDBInfoMemoryUsage{}
