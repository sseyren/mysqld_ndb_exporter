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

// Scrape `ndbinfo.pgman_time_track_stats`.

package collector

import (
	"context"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

// TODO add more verbose metrics

const ndbinfoPGMANTimeTrackStatsQuery = `
	SELECT
		node_id,
		upper_bound,
		SUM(page_reads) AS page_reads,
		SUM(page_writes) AS page_writes,
		SUM(log_waits) AS log_waits,
		SUM(get_page) AS get_page
	FROM ndbinfo.pgman_time_track_stats
	GROUP BY node_id, upper_bound;
`

// Metric descriptors.
var (
	ndbinfoPGMANTimeTrackStatsHistogramHelper ndbinfoHistogramHelper = []float64{0, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576, 2097152, 4294967295}

	ndbinfoPGMANTimeTrackStatsDescPageReads = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "pgman_time_track_stats_page_reads"),
		"Total number of page reads by latency as ms.",
		[]string{"node_id"}, nil,
	)
	ndbinfoPGMANTimeTrackStatsDescPageWrites = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "pgman_time_track_stats_page_writes"),
		"Total number of page writes by latency as ms.",
		[]string{"node_id"}, nil,
	)
	ndbinfoPGMANTimeTrackStatsDescLogWaits = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "pgman_time_track_stats_log_waits"),
		"Total number of undo log waits by latency as ms.",
		[]string{"node_id"}, nil,
	)
	ndbinfoPGMANTimeTrackStatsDescGetPageCalls = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "pgman_time_track_stats_get_page_calls"),
		"Total number of get_page() calls by latency as ms.",
		[]string{"node_id"}, nil,
	)
)

// ScrapeNDBInfoPGMANTimeTrackStats collects from `ndbinfo.pgman_time_track_stats`.
type ScrapeNDBInfoPGMANTimeTrackStats struct{}

// Name of the Scraper. Should be unique.
func (ScrapeNDBInfoPGMANTimeTrackStats) Name() string {
	return ndbInfo + ".pgman_time_track_stats"
}

// Help describes the role of the Scraper.
func (ScrapeNDBInfoPGMANTimeTrackStats) Help() string {
	return "Collect metrics from ndbinfo.pgman_time_track_stats"
}

// Version of MySQL from which scraper is available.
func (ScrapeNDBInfoPGMANTimeTrackStats) Version() float64 {
	return 8.0
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeNDBInfoPGMANTimeTrackStats) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, ndbinfoPGMANTimeTrackStatsQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	lastUpperBound := ndbinfoPGMANTimeTrackStatsHistogramHelper.GetLast()
	var (
		nodeID                                   string
		upperBound                               float64
		pageReads, pageWrites, logWaits, getPage uint64
	)
	// Histogram buckets by nodeID
	pageReadsBuckets := map[string]map[float64]uint64{}
	pageWritesBuckets := map[string]map[float64]uint64{}
	logWaitsBuckets := map[string]map[float64]uint64{}
	getPageBuckets := map[string]map[float64]uint64{}

	for rows.Next() {
		if err := rows.Scan(
			&nodeID, &upperBound,
			&pageReads, &pageWrites, &logWaits, &getPage,
		); err != nil {
			return err
		}
		if pageReadsBuckets[nodeID] == nil {
			pageReadsBuckets[nodeID] = make(map[float64]uint64, 20)
		}
		pageReadsBuckets[nodeID][upperBound] = pageReads

		if pageWritesBuckets[nodeID] == nil {
			pageWritesBuckets[nodeID] = make(map[float64]uint64, 20)
		}
		pageWritesBuckets[nodeID][upperBound] = pageWrites

		if logWaitsBuckets[nodeID] == nil {
			logWaitsBuckets[nodeID] = make(map[float64]uint64, 20)
		}
		logWaitsBuckets[nodeID][upperBound] = logWaits

		if getPageBuckets[nodeID] == nil {
			getPageBuckets[nodeID] = make(map[float64]uint64, 20)
		}
		getPageBuckets[nodeID][upperBound] = getPage
	}

	for nodeID, bucket := range pageReadsBuckets {
		sum := ndbinfoPGMANTimeTrackStatsHistogramHelper.ArtificialSum(bucket)
		ndbinfoPGMANTimeTrackStatsHistogramHelper.MakeCumulative(bucket)
		ch <- prometheus.MustNewConstHistogram(ndbinfoPGMANTimeTrackStatsDescPageReads, bucket[lastUpperBound], sum, bucket,
			nodeID,
		)
	}
	for nodeID, bucket := range pageWritesBuckets {
		sum := ndbinfoPGMANTimeTrackStatsHistogramHelper.ArtificialSum(bucket)
		ndbinfoPGMANTimeTrackStatsHistogramHelper.MakeCumulative(bucket)
		ch <- prometheus.MustNewConstHistogram(ndbinfoPGMANTimeTrackStatsDescPageWrites, bucket[lastUpperBound], sum, bucket,
			nodeID,
		)
	}
	for nodeID, bucket := range logWaitsBuckets {
		sum := ndbinfoPGMANTimeTrackStatsHistogramHelper.ArtificialSum(bucket)
		ndbinfoPGMANTimeTrackStatsHistogramHelper.MakeCumulative(bucket)
		ch <- prometheus.MustNewConstHistogram(ndbinfoPGMANTimeTrackStatsDescLogWaits, bucket[lastUpperBound], sum, bucket,
			nodeID,
		)
	}
	for nodeID, bucket := range getPageBuckets {
		sum := ndbinfoPGMANTimeTrackStatsHistogramHelper.ArtificialSum(bucket)
		ndbinfoPGMANTimeTrackStatsHistogramHelper.MakeCumulative(bucket)
		ch <- prometheus.MustNewConstHistogram(ndbinfoPGMANTimeTrackStatsDescGetPageCalls, bucket[lastUpperBound], sum, bucket,
			nodeID,
		)
	}

	return nil
}

// check interface
var _ Scraper = ScrapeNDBInfoPGMANTimeTrackStats{}
