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

// Scrape `ndbinfo.threadstat`.

package collector

import (
	"context"
	"log/slog"

	"github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/client_golang/prometheus"
)

const ndbinfoThreadstatQuery = `
	SELECT
		node_id,
		thr_no,
		thr_nm,
		c_loop,
		c_exec,
		c_wait,
		os_tid,
		os_ru_minflt,
		os_ru_majflt,
		os_ru_nvcsw,
		os_ru_nivcsw
	FROM ndbinfo.threadstat;
`

const ndbinfoThreadstatQueryFull = `
	SELECT
		node_id,
		thr_no,
		thr_nm,
		c_loop,
		c_exec,
		c_wait,
		c_l_sent_prioa,
		c_l_sent_priob,
		c_r_sent_prioa,
		c_r_sent_priob,
		os_tid,
		os_now,
		os_ru_utime,
		os_ru_stime,
		os_ru_minflt,
		os_ru_majflt,
		os_ru_nvcsw,
		os_ru_nivcsw
	FROM ndbinfo.threadstat;
`

// Tunable flags.
var (
	ndbinfoThreadstatVerbosityFlag = kingpin.Flag(
		"collect."+ScrapeNDBInfoThreadstat{}.Name()+".verbose",
		"Metrics verbosity",
	).Default("0").Enum("0", "1")
)

// Metric descriptors.
var (
	ndbinfoThreadstatDescLoops = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "threadstat_loops"),
		"Number of loops in main loop.",
		[]string{"node_id", "thr_no", "thr_nm", "os_tid"}, nil,
	)
	ndbinfoThreadstatDescSignalsExec = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "threadstat_signals_exec"),
		"Number of signals executed.",
		[]string{"node_id", "thr_no", "thr_nm", "os_tid"}, nil,
	)
	ndbinfoThreadstatDescWaits = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "threadstat_waits"),
		"Number of times waiting for additional input.",
		[]string{"node_id", "thr_no", "thr_nm", "os_tid"}, nil,
	)
	ndbinfoThreadstatDescSignalsSentLocalPrioA = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "threadstat_signals_sent_local_prioa"),
		"Number of priority A signals sent to own node.",
		[]string{"node_id", "thr_no", "thr_nm", "os_tid"}, nil,
	)
	ndbinfoThreadstatDescSignalsSentLocalPrioB = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "threadstat_signals_sent_local_priob"),
		"Number of priority B signals sent to own node.",
		[]string{"node_id", "thr_no", "thr_nm", "os_tid"}, nil,
	)
	ndbinfoThreadstatDescSignalsSentRemotePrioA = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "threadstat_signals_sent_remote_prioa"),
		"Number of priority A signals sent to remote node.",
		[]string{"node_id", "thr_no", "thr_nm", "os_tid"}, nil,
	)
	ndbinfoThreadstatDescSignalsSentRemotePrioB = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "threadstat_signals_sent_remote_priob"),
		"Number of priority B signals sent to remote node.",
		[]string{"node_id", "thr_no", "thr_nm", "os_tid"}, nil,
	)
	ndbinfoThreadstatDescOsNow = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "threadstat_os_now"),
		"OS time (gettimeofday) in ms.",
		[]string{"node_id", "thr_no", "thr_nm", "os_tid"}, nil,
	)
	ndbinfoThreadstatDescOsUtime = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "threadstat_os_utime"),
		"OS user CPU time in µs.",
		[]string{"node_id", "thr_no", "thr_nm", "os_tid"}, nil,
	)
	ndbinfoThreadstatDescOsStime = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "threadstat_os_stime"),
		"OS system CPU time in µs.",
		[]string{"node_id", "thr_no", "thr_nm", "os_tid"}, nil,
	)
	ndbinfoThreadstatDescOsMinflt = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "threadstat_os_minflt"),
		"OS page reclaims (soft page faults).",
		[]string{"node_id", "thr_no", "thr_nm", "os_tid"}, nil,
	)
	ndbinfoThreadstatDescOsMajflt = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "threadstat_os_majflt"),
		"OS page faults (hard page faults).",
		[]string{"node_id", "thr_no", "thr_nm", "os_tid"}, nil,
	)
	ndbinfoThreadstatDescOsNvcsw = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "threadstat_os_nvcsw"),
		"OS voluntary context switches.",
		[]string{"node_id", "thr_no", "thr_nm", "os_tid"}, nil,
	)
	ndbinfoThreadstatDescOsNivcsw = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "threadstat_os_nivcsw"),
		"OS involuntary context switches.",
		[]string{"node_id", "thr_no", "thr_nm", "os_tid"}, nil,
	)
)

// ScrapeNDBInfoThreadstat collects from `ndbinfo.threadstat`.
type ScrapeNDBInfoThreadstat struct{}

// Name of the Scraper. Should be unique.
func (ScrapeNDBInfoThreadstat) Name() string {
	return ndbInfo + ".threadstat"
}

// Help describes the role of the Scraper.
func (ScrapeNDBInfoThreadstat) Help() string {
	return "Collect metrics from ndbinfo.threadstat"
}

// Version of MySQL from which scraper is available.
func (ScrapeNDBInfoThreadstat) Version() float64 {
	return 5.6
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (self ScrapeNDBInfoThreadstat) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	switch *ndbinfoThreadstatVerbosityFlag {
	case "1":
		return self.ScrapeVerbose1(ctx, instance, ch, logger)
	default:
		return self.ScrapeVerbose0(ctx, instance, ch, logger)
	}
}

func (ScrapeNDBInfoThreadstat) ScrapeVerbose0(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, ndbinfoThreadstatQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		nodeID, thrNo, thrNm                          string
		cLoop, cExec, cWait                           uint64
		osTID                                         string
		osRuMinflt, osRuMajflt, osRuNvcsw, osRuNivcsw uint64
	)
	for rows.Next() {
		if err := rows.Scan(
			&nodeID, &thrNo, &thrNm,
			&cLoop, &cExec, &cWait,
			&osTID,
			&osRuMinflt, &osRuMajflt, &osRuNvcsw, &osRuNivcsw,
		); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			ndbinfoThreadstatDescLoops, prometheus.CounterValue, float64(cLoop),
			nodeID, thrNo, thrNm, osTID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoThreadstatDescSignalsExec, prometheus.CounterValue, float64(cExec),
			nodeID, thrNo, thrNm, osTID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoThreadstatDescWaits, prometheus.CounterValue, float64(cWait),
			nodeID, thrNo, thrNm, osTID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoThreadstatDescOsMinflt, prometheus.CounterValue, float64(osRuMinflt),
			nodeID, thrNo, thrNm, osTID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoThreadstatDescOsMajflt, prometheus.CounterValue, float64(osRuMajflt),
			nodeID, thrNo, thrNm, osTID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoThreadstatDescOsNvcsw, prometheus.CounterValue, float64(osRuNvcsw),
			nodeID, thrNo, thrNm, osTID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoThreadstatDescOsNivcsw, prometheus.CounterValue, float64(osRuNivcsw),
			nodeID, thrNo, thrNm, osTID,
		)
	}

	return nil
}

func (ScrapeNDBInfoThreadstat) ScrapeVerbose1(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, ndbinfoThreadstatQueryFull)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		nodeID, thrNo, thrNm                                                       string
		cLoop, cExec, cWait, cLSentPrioa, cLSentPriob, cRSentPrioa, cRSentPriob    uint64
		osTID                                                                      string
		osNow, osRuUtime, osRuStime, osRuMinflt, osRuMajflt, osRuNvcsw, osRuNivcsw uint64
	)
	for rows.Next() {
		if err := rows.Scan(
			&nodeID, &thrNo, &thrNm,
			&cLoop, &cExec, &cWait, &cLSentPrioa, &cLSentPriob, &cRSentPrioa, &cRSentPriob,
			&osTID,
			&osNow, &osRuUtime, &osRuStime, &osRuMinflt, &osRuMajflt, &osRuNvcsw, &osRuNivcsw,
		); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			ndbinfoThreadstatDescLoops, prometheus.CounterValue, float64(cLoop),
			nodeID, thrNo, thrNm, osTID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoThreadstatDescSignalsExec, prometheus.CounterValue, float64(cExec),
			nodeID, thrNo, thrNm, osTID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoThreadstatDescWaits, prometheus.CounterValue, float64(cWait),
			nodeID, thrNo, thrNm, osTID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoThreadstatDescSignalsSentLocalPrioA, prometheus.CounterValue, float64(cLSentPrioa),
			nodeID, thrNo, thrNm, osTID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoThreadstatDescSignalsSentLocalPrioB, prometheus.CounterValue, float64(cLSentPriob),
			nodeID, thrNo, thrNm, osTID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoThreadstatDescSignalsSentRemotePrioA, prometheus.CounterValue, float64(cRSentPrioa),
			nodeID, thrNo, thrNm, osTID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoThreadstatDescSignalsSentRemotePrioB, prometheus.CounterValue, float64(cRSentPriob),
			nodeID, thrNo, thrNm, osTID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoThreadstatDescOsNow, prometheus.CounterValue, float64(osNow),
			nodeID, thrNo, thrNm, osTID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoThreadstatDescOsUtime, prometheus.CounterValue, float64(osRuUtime),
			nodeID, thrNo, thrNm, osTID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoThreadstatDescOsStime, prometheus.CounterValue, float64(osRuStime),
			nodeID, thrNo, thrNm, osTID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoThreadstatDescOsMinflt, prometheus.CounterValue, float64(osRuMinflt),
			nodeID, thrNo, thrNm, osTID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoThreadstatDescOsMajflt, prometheus.CounterValue, float64(osRuMajflt),
			nodeID, thrNo, thrNm, osTID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoThreadstatDescOsNvcsw, prometheus.CounterValue, float64(osRuNvcsw),
			nodeID, thrNo, thrNm, osTID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoThreadstatDescOsNivcsw, prometheus.CounterValue, float64(osRuNivcsw),
			nodeID, thrNo, thrNm, osTID,
		)
	}

	return nil
}

// check interface
var _ Scraper = ScrapeNDBInfoThreadstat{}
