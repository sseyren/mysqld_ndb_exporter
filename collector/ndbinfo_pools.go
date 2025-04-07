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

// Scrape `ndbinfo.ndb$pools`.

package collector

import (
	"context"
	"log/slog"

	"github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/client_golang/prometheus"
)

const ndbinfoPoolsQuery = `
	SELECT
		pool_name,
		SUM(used * entry_size) AS used,
		SUM(total * entry_size) AS total
	FROM ndbinfo.ndb$pools
	GROUP BY pool_name;
`

const ndbinfoPoolsQueryAgg1 = `
	SELECT
		node_id,
		pool_name,
		SUM(used * entry_size) AS used,
		SUM(total * entry_size) AS total
	FROM ndbinfo.ndb$pools
	GROUP BY node_id, pool_name;
`

const ndbinfoPoolsQueryFull = `
	SELECT
		p.node_id,
		b.block_name,
		p.block_instance,
		p.pool_name,
		p.used * p.entry_size AS used,
		p.total * p.entry_size AS total
	FROM ndbinfo.ndb$pools p
	LEFT JOIN ndbinfo.blocks b ON p.block_number = b.block_number;
`

// Tunable flags.
var (
	ndbinfoPoolsVerbosityFlag = kingpin.Flag(
		"collect."+ScrapeNDBInfoPools{}.Name()+".verbose",
		"Metrics verbosity",
	).Default("0").Enum("0", "1", "2")
)

// Metric descriptors.
var (
	ndbinfoPoolsDescUsed = NewVariablePromDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "pools_used"),
		"Number of bytes currently used for this pool.",
		prometheus.GaugeValue,
		[]string{"pool_name"},
		[]string{"node_id", "pool_name"},
		[]string{"node_id", "block_name", "block_instance", "pool_name"},
	)
	ndbinfoPoolsDescTotal = NewVariablePromDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "pools_total"),
		"Total number of bytes available for this pool.",
		prometheus.GaugeValue,
		[]string{"pool_name"},
		[]string{"node_id", "pool_name"},
		[]string{"node_id", "block_name", "block_instance", "pool_name"},
	)
)

// ScrapeNDBInfoPools collects from `ndbinfo.ndb$pools`.
type ScrapeNDBInfoPools struct{}

// Name of the Scraper. Should be unique.
func (ScrapeNDBInfoPools) Name() string {
	return ndbInfo + ".pools"
}

// Help describes the role of the Scraper.
func (ScrapeNDBInfoPools) Help() string {
	return "Collect metrics from ndbinfo.ndb$pools"
}

// Version of MySQL from which scraper is available.
func (ScrapeNDBInfoPools) Version() float64 {
	return 5.6
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (self ScrapeNDBInfoPools) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	switch *ndbinfoPoolsVerbosityFlag {
	case "2":
		return self.ScrapeVerbose2(ctx, instance, ch, logger)
	case "1":
		return self.ScrapeVerbose1(ctx, instance, ch, logger)
	default:
		return self.ScrapeVerbose0(ctx, instance, ch, logger)
	}
}

func (ScrapeNDBInfoPools) ScrapeVerbose0(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, ndbinfoPoolsQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		poolName    string
		used, total uint64
	)
	for rows.Next() {
		if err := rows.Scan(
			&poolName,
			&used, &total,
		); err != nil {
			return err
		}
		ch <- ndbinfoPoolsDescUsed.Metric(float64(used), poolName)
		ch <- ndbinfoPoolsDescTotal.Metric(float64(total), poolName)
	}

	return nil
}

func (ScrapeNDBInfoPools) ScrapeVerbose1(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, ndbinfoPoolsQueryAgg1)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		nodeID, poolName string
		used, total      uint64
	)
	for rows.Next() {
		if err := rows.Scan(
			&nodeID, &poolName,
			&used, &total,
		); err != nil {
			return err
		}
		ch <- ndbinfoPoolsDescUsed.Metric(float64(used), nodeID, poolName)
		ch <- ndbinfoPoolsDescTotal.Metric(float64(total), nodeID, poolName)
	}

	return nil
}

func (ScrapeNDBInfoPools) ScrapeVerbose2(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, ndbinfoPoolsQueryFull)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		nodeID, blockName, blockInstance, poolName string
		used, total                                uint64
	)
	for rows.Next() {
		if err := rows.Scan(
			&nodeID, &blockName, &blockInstance, &poolName,
			&used, &total,
		); err != nil {
			return err
		}
		ch <- ndbinfoPoolsDescUsed.Metric(float64(used), nodeID, blockName, blockInstance, poolName)
		ch <- ndbinfoPoolsDescTotal.Metric(float64(total), nodeID, blockName, blockInstance, poolName)
	}

	return nil
}

// check interface
var _ Scraper = ScrapeNDBInfoPools{}
