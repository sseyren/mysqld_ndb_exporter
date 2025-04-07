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

// Scrape `ndbinfo.resources`.

package collector

import (
	"context"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

const ndbinfoResourcesQuery = `
	SELECT
		node_id,
		resource_name,
		reserved,
		used,
		max
	FROM ndbinfo.resources;
`

// Metric descriptors.
var (
	ndbinfoResourcesDescReserved = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "resources_reserved"),
		"The amount reserved for this resource, as a number of 32KB pages.",
		[]string{"node_id", "resource_name"}, nil,
	)
	ndbinfoResourcesDescUsed = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "resources_used"),
		"The amount actually used by this resource, as a number of 32KB pages.",
		[]string{"node_id", "resource_name"}, nil,
	)
	ndbinfoResourcesDescMax = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "resources_max"),
		"The maximum amount (number of 32KB pages) of this resource that is available to this data node.",
		[]string{"node_id", "resource_name"}, nil,
	)
)

// ScrapeNDBInfoResources collects from `ndbinfo.resources`.
type ScrapeNDBInfoResources struct{}

// Name of the Scraper. Should be unique.
func (ScrapeNDBInfoResources) Name() string {
	return ndbInfo + ".resources"
}

// Help describes the role of the Scraper.
func (ScrapeNDBInfoResources) Help() string {
	return "Collect metrics from ndbinfo.resources"
}

// Version of MySQL from which scraper is available.
func (ScrapeNDBInfoResources) Version() float64 {
	return 5.6
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeNDBInfoResources) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, ndbinfoResourcesQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		nodeID, resourceName string
		reserved, used, max  uint64
	)
	for rows.Next() {
		if err := rows.Scan(
			&nodeID, &resourceName,
			&reserved, &used, &max,
		); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			ndbinfoResourcesDescReserved, prometheus.GaugeValue, float64(reserved),
			nodeID, resourceName,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoResourcesDescUsed, prometheus.GaugeValue, float64(used),
			nodeID, resourceName,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoResourcesDescMax, prometheus.GaugeValue, float64(max),
			nodeID, resourceName,
		)
	}

	return nil
}

// check interface
var _ Scraper = ScrapeNDBInfoResources{}
