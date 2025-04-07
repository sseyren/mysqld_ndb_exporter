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

// Scrape `ndbinfo.counters`.

package collector

import (
	"context"
	"log/slog"

	"github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/client_golang/prometheus"
)

const ndbinfoCountersQuery = `
	SELECT
		node_id,
		block_name,
		counter_name,
		SUM(val) as val
	FROM ndbinfo.counters
	GROUP BY node_id, block_name, counter_name;
`

const ndbinfoCountersQueryFull = `
	SELECT
		node_id,
		block_name,
		block_instance,
		counter_name,
		val
	FROM ndbinfo.counters;
`

// Tunable flags.
var (
	ndbinfoCountersVerbosityFlag = kingpin.Flag(
		"collect."+ScrapeNDBInfoCounters{}.Name()+".verbose",
		"Metrics verbosity",
	).Default("0").Enum("0", "1")
)

// Metric descriptors.
var (
	ndbinfoCountersDesc = NewVariablePromDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "counters"),
		"Running totals of events such as reads and writes for specific kernel blocks and data nodes.",
		prometheus.CounterValue,
		[]string{"node_id", "block_name", "counter_name"},
		[]string{"node_id", "block_name", "block_instance", "counter_name"},
	)
)

// ScrapeNDBInfoCounters collects from `ndbinfo.counters`.
type ScrapeNDBInfoCounters struct{}

// Name of the Scraper. Should be unique.
func (ScrapeNDBInfoCounters) Name() string {
	return ndbInfo + ".counters"
}

// Help describes the role of the Scraper.
func (ScrapeNDBInfoCounters) Help() string {
	return "Collect metrics from ndbinfo.counters"
}

// Version of MySQL from which scraper is available.
func (ScrapeNDBInfoCounters) Version() float64 {
	return 5.6
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (self ScrapeNDBInfoCounters) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	switch *ndbinfoCountersVerbosityFlag {
	case "1":
		return self.ScrapeVerbose1(ctx, instance, ch, logger)
	default:
		return self.ScrapeVerbose0(ctx, instance, ch, logger)
	}
}

func (ScrapeNDBInfoCounters) ScrapeVerbose0(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, ndbinfoCountersQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		nodeID, blockName, counterName string
		val                            uint64
	)
	for rows.Next() {
		if err := rows.Scan(
			&nodeID, &blockName, &counterName,
			&val,
		); err != nil {
			return err
		}
		ch <- ndbinfoCountersDesc.Metric(float64(val), nodeID, blockName, counterName)
	}

	return nil
}

func (ScrapeNDBInfoCounters) ScrapeVerbose1(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, ndbinfoCountersQueryFull)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		nodeID, blockName, blockInstance, counterName string
		val                                           uint64
	)
	for rows.Next() {
		if err := rows.Scan(
			&nodeID, &blockName, &blockInstance, &counterName,
			&val,
		); err != nil {
			return err
		}
		ch <- ndbinfoCountersDesc.Metric(float64(val), nodeID, blockName, blockInstance, counterName)
	}

	return nil
}

// check interface
var _ Scraper = ScrapeNDBInfoCounters{}
