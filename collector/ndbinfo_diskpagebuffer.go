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

// Scrape `ndbinfo.diskpagebuffer`.

package collector

import (
	"context"
	"log/slog"

	"github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/client_golang/prometheus"
)

const ndbinfoDiskPageBufferQuery = `
	SELECT
		node_id,
		SUM(pages_written) AS pages_written,
		SUM(pages_written_lcp) AS pages_written_lcp,
		SUM(pages_read) AS pages_read,
		SUM(log_waits) AS log_waits,
		SUM(page_requests_direct_return) AS page_requests_direct_return,
		SUM(page_requests_wait_queue) AS page_requests_wait_queue,
		SUM(page_requests_wait_io) AS page_requests_wait_io
	FROM ndbinfo.diskpagebuffer
	GROUP BY node_id;
`

const ndbinfoDiskPageBufferQueryFull = `
	SELECT
		node_id,
		block_instance,
		pages_written,
		pages_written_lcp,
		pages_read,
		log_waits,
		page_requests_direct_return,
		page_requests_wait_queue,
		page_requests_wait_io
	FROM ndbinfo.diskpagebuffer;
`

// Tunable flags.
var (
	ndbinfoDiskPageBufferVerbosityFlag = kingpin.Flag(
		"collect."+ScrapeNDBInfoDiskPageBuffer{}.Name()+".verbose",
		"Metrics verbosity",
	).Default("0").Enum("0", "1")
)

// Metric descriptors.
var (
	ndbinfoDiskPageBufferLabels     = []string{"node_id"}
	ndbinfoDiskPageBufferLabelsFull = append(ndbinfoDiskPageBufferLabels, "block_instance")

	ndbinfoDiskPageBufferDescPagesWritten = NewVariablePromDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "diskpagebuffer_pages_written"),
		"Number of pages written to disk.",
		prometheus.CounterValue,
		ndbinfoDiskPageBufferLabels, ndbinfoDiskPageBufferLabelsFull,
	)
	ndbinfoDiskPageBufferDescPagesWrittenLCP = NewVariablePromDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "diskpagebuffer_pages_written_lcp"),
		"Number of pages written by local checkpoints.",
		prometheus.CounterValue,
		ndbinfoDiskPageBufferLabels, ndbinfoDiskPageBufferLabelsFull,
	)
	ndbinfoDiskPageBufferDescPagesRead = NewVariablePromDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "diskpagebuffer_pages_read"),
		"Number of pages read from disk.",
		prometheus.CounterValue,
		ndbinfoDiskPageBufferLabels, ndbinfoDiskPageBufferLabelsFull,
	)
	ndbinfoDiskPageBufferDescLogWaits = NewVariablePromDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "diskpagebuffer_log_waits"),
		"Number of page writes waiting for log to be written to disk.",
		prometheus.CounterValue,
		ndbinfoDiskPageBufferLabels, ndbinfoDiskPageBufferLabelsFull,
	)
	ndbinfoDiskPageBufferDescPageRequestDirectReturn = NewVariablePromDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "diskpagebuffer_page_requests_direct_return"),
		"Number of requests for pages that were available in buffer.",
		prometheus.CounterValue,
		ndbinfoDiskPageBufferLabels, ndbinfoDiskPageBufferLabelsFull,
	)
	ndbinfoDiskPageBufferDescPageRequestWaitQueue = NewVariablePromDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "diskpagebuffer_page_requests_wait_queue"),
		"Number of requests that had to wait for pages to become available in buffer.",
		prometheus.CounterValue,
		ndbinfoDiskPageBufferLabels, ndbinfoDiskPageBufferLabelsFull,
	)
	ndbinfoDiskPageBufferDescPageRequestWaitIO = NewVariablePromDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "diskpagebuffer_page_requests_wait_io"),
		"Number of requests that had to be read from pages on disk (pages were unavailable in buffer).",
		prometheus.CounterValue,
		ndbinfoDiskPageBufferLabels, ndbinfoDiskPageBufferLabelsFull,
	)
)

// ScrapeNDBInfoDiskPageBuffer collects from `ndbinfo.diskpagebuffer`.
type ScrapeNDBInfoDiskPageBuffer struct{}

// Name of the Scraper. Should be unique.
func (ScrapeNDBInfoDiskPageBuffer) Name() string {
	return ndbInfo + ".diskpagebuffer"
}

// Help describes the role of the Scraper.
func (ScrapeNDBInfoDiskPageBuffer) Help() string {
	return "Collect metrics from ndbinfo.diskpagebuffer"
}

// Version of MySQL from which scraper is available.
func (ScrapeNDBInfoDiskPageBuffer) Version() float64 {
	return 5.6
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (self ScrapeNDBInfoDiskPageBuffer) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	switch *ndbinfoDiskPageBufferVerbosityFlag {
	case "1":
		return self.ScrapeVerbose1(ctx, instance, ch, logger)
	default:
		return self.ScrapeVerbose0(ctx, instance, ch, logger)
	}
}

func (ScrapeNDBInfoDiskPageBuffer) ScrapeVerbose0(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, ndbinfoDiskPageBufferQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		nodeID                                                              string
		pagesWritten, pagesWrittenLCP, pagesRead, logWaits                  uint64
		pageRequestsDirectReturn, pageRequestsWaitQueue, pageRequestsWaitIO uint64
	)
	for rows.Next() {
		if err := rows.Scan(
			&nodeID,
			&pagesWritten, &pagesWrittenLCP, &pagesRead, &logWaits,
			&pageRequestsDirectReturn, &pageRequestsWaitQueue, &pageRequestsWaitIO,
		); err != nil {
			return err
		}
		ch <- ndbinfoDiskPageBufferDescPagesWritten.Metric(float64(pagesWritten), nodeID)
		ch <- ndbinfoDiskPageBufferDescPagesWrittenLCP.Metric(float64(pagesWrittenLCP), nodeID)
		ch <- ndbinfoDiskPageBufferDescPagesRead.Metric(float64(pagesRead), nodeID)
		ch <- ndbinfoDiskPageBufferDescLogWaits.Metric(float64(logWaits), nodeID)
		ch <- ndbinfoDiskPageBufferDescPageRequestDirectReturn.Metric(float64(pageRequestsDirectReturn), nodeID)
		ch <- ndbinfoDiskPageBufferDescPageRequestWaitQueue.Metric(float64(pageRequestsWaitQueue), nodeID)
		ch <- ndbinfoDiskPageBufferDescPageRequestWaitIO.Metric(float64(pageRequestsWaitIO), nodeID)
	}

	return nil
}

func (ScrapeNDBInfoDiskPageBuffer) ScrapeVerbose1(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, ndbinfoDiskPageBufferQueryFull)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		nodeID, blockInstance                                               string
		pagesWritten, pagesWrittenLCP, pagesRead, logWaits                  uint64
		pageRequestsDirectReturn, pageRequestsWaitQueue, pageRequestsWaitIO uint64
	)
	for rows.Next() {
		if err := rows.Scan(
			&nodeID, &blockInstance,
			&pagesWritten, &pagesWrittenLCP, &pagesRead, &logWaits,
			&pageRequestsDirectReturn, &pageRequestsWaitQueue, &pageRequestsWaitIO,
		); err != nil {
			return err
		}
		ch <- ndbinfoDiskPageBufferDescPagesWritten.Metric(float64(pagesWritten), nodeID, blockInstance)
		ch <- ndbinfoDiskPageBufferDescPagesWrittenLCP.Metric(float64(pagesWrittenLCP), nodeID, blockInstance)
		ch <- ndbinfoDiskPageBufferDescPagesRead.Metric(float64(pagesRead), nodeID, blockInstance)
		ch <- ndbinfoDiskPageBufferDescLogWaits.Metric(float64(logWaits), nodeID, blockInstance)
		ch <- ndbinfoDiskPageBufferDescPageRequestDirectReturn.Metric(float64(pageRequestsDirectReturn), nodeID, blockInstance)
		ch <- ndbinfoDiskPageBufferDescPageRequestWaitQueue.Metric(float64(pageRequestsWaitQueue), nodeID, blockInstance)
		ch <- ndbinfoDiskPageBufferDescPageRequestWaitIO.Metric(float64(pageRequestsWaitIO), nodeID, blockInstance)
	}

	return nil
}

// check interface
var _ Scraper = ScrapeNDBInfoDiskPageBuffer{}
