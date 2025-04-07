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

// Scrape `ndbinfo.diskstat`.

package collector

import (
	"context"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

// TODO this scraper works wrong.
// we need a background task that scrape ndbinfo.diskstat on every second.
// or ndbinfo.diskstats_1sec on every 15 secs. (i'm not sure diskstats_1sec works correctly tbh)
// this task must collect value and update a shared struct.
// whenever actual Scrape() method invoked, we need to get values from shared struct and reset.

const ndbinfoDiskStatQuery = `
	SELECT
		node_id,
		block_instance,
		pages_made_dirty,
		reads_issued,
		reads_completed,
		writes_issued,
		writes_completed,
		log_writes_issued,
		log_writes_completed,
		get_page_calls_issued,
		get_page_reqs_issued,
		get_page_reqs_completed
	FROM ndbinfo.diskstat;
`

// Metric descriptors.
var (
	ndbinfoDiskStatDescPagesMadeDirty = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "diskstat_pages_made_dirty"),
		"Number of pages made dirty during the past second.",
		[]string{"node_id", "block_instance"}, nil,
	)
	ndbinfoDiskStatDescReadsIssued = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "diskstat_reads_issued"),
		"Reads issued during the past second.",
		[]string{"node_id", "block_instance"}, nil,
	)
	ndbinfoDiskStatDescReadsCompleted = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "diskstat_reads_completed"),
		"Reads completed during the past second.",
		[]string{"node_id", "block_instance"}, nil,
	)
	ndbinfoDiskStatDescWritesIssued = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "diskstat_writes_issued"),
		"Writes issued during the past second.",
		[]string{"node_id", "block_instance"}, nil,
	)
	ndbinfoDiskStatDescWritesCompleted = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "diskstat_writes_completed"),
		"Writes completed during the past second.",
		[]string{"node_id", "block_instance"}, nil,
	)
	ndbinfoDiskStatDescLogWritesIssued = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "diskstat_log_writes_issued"),
		"Number of times a page write has required a log write during the past second.",
		[]string{"node_id", "block_instance"}, nil,
	)
	ndbinfoDiskStatDescLogWritesCompleted = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "diskstat_log_writes_completed"),
		"Number of log writes completed during the last second.",
		[]string{"node_id", "block_instance"}, nil,
	)
	ndbinfoDiskStatDescGetPageCallsIssued = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "diskstat_get_page_calls_issued"),
		"Number of get_page() calls issued during the past second.",
		[]string{"node_id", "block_instance"}, nil,
	)
	ndbinfoDiskStatDescGetPageReqsIssued = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "diskstat_get_page_reqs_issued"),
		"Number of times that a get_page() call has resulted in a wait for I/O or completion of I/O already begun during the past second.",
		[]string{"node_id", "block_instance"}, nil,
	)
	ndbinfoDiskStatDescGetPageReqsCompleted = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "diskstat_get_page_reqs_completed"),
		"Number of get_page() calls waiting for I/O or I/O completion that have completed during the past second.",
		[]string{"node_id", "block_instance"}, nil,
	)
)

// ScrapeNDBInfoDiskStat collects from `ndbinfo.diskstat`.
type ScrapeNDBInfoDiskStat struct{}

// Name of the Scraper. Should be unique.
func (ScrapeNDBInfoDiskStat) Name() string {
	return ndbInfo + ".diskstat"
}

// Help describes the role of the Scraper.
func (ScrapeNDBInfoDiskStat) Help() string {
	return "Collect metrics from ndbinfo.diskstat"
}

// Version of MySQL from which scraper is available.
func (ScrapeNDBInfoDiskStat) Version() float64 {
	return 8.0
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeNDBInfoDiskStat) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	ndbinfoDiskStatRows, err := db.QueryContext(ctx, ndbinfoDiskStatQuery)
	if err != nil {
		return err
	}
	defer ndbinfoDiskStatRows.Close()

	var (
		nodeID, blockInstance                          string
		pagesMadeDirty, readsIssued, readsCompleted    uint64
		writesIssued, writesCompleted, logWritesIssued uint64
		logWritesCompleted, getPageCallsIssued         uint64
		getPageReqsIssued, getPageReqsCompleted        uint64
	)
	for ndbinfoDiskStatRows.Next() {
		if err := ndbinfoDiskStatRows.Scan(
			&nodeID, &blockInstance,
			&pagesMadeDirty, &readsIssued, &readsCompleted,
			&writesIssued, &writesCompleted, &logWritesIssued,
			&logWritesCompleted, &getPageCallsIssued,
			&getPageReqsIssued, &getPageReqsCompleted,
		); err != nil {
			return err
		}
		// TODO CounterValue?
		ch <- prometheus.MustNewConstMetric(
			ndbinfoDiskStatDescPagesMadeDirty, prometheus.GaugeValue, float64(pagesMadeDirty),
			nodeID, blockInstance,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoDiskStatDescReadsIssued, prometheus.GaugeValue, float64(readsIssued),
			nodeID, blockInstance,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoDiskStatDescReadsCompleted, prometheus.GaugeValue, float64(readsCompleted),
			nodeID, blockInstance,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoDiskStatDescWritesIssued, prometheus.GaugeValue, float64(writesIssued),
			nodeID, blockInstance,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoDiskStatDescWritesCompleted, prometheus.GaugeValue, float64(writesCompleted),
			nodeID, blockInstance,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoDiskStatDescLogWritesIssued, prometheus.GaugeValue, float64(logWritesIssued),
			nodeID, blockInstance,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoDiskStatDescLogWritesCompleted, prometheus.GaugeValue, float64(logWritesCompleted),
			nodeID, blockInstance,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoDiskStatDescGetPageCallsIssued, prometheus.GaugeValue, float64(getPageCallsIssued),
			nodeID, blockInstance,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoDiskStatDescGetPageReqsIssued, prometheus.GaugeValue, float64(getPageReqsIssued),
			nodeID, blockInstance,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoDiskStatDescGetPageReqsCompleted, prometheus.GaugeValue, float64(getPageReqsCompleted),
			nodeID, blockInstance,
		)
	}

	return nil
}

// check interface
var _ Scraper = ScrapeNDBInfoDiskStat{}
