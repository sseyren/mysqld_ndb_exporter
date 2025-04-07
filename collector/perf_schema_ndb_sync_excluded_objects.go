// Copyright 2020 The Prometheus Authors
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

package collector

import (
	"context"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

// TODO add more verbose metrics

const perfNDBSyncExcludedObjectsQuery = `
	SELECT COUNT(*) AS object_count
	FROM performance_schema.ndb_sync_excluded_objects;
`

// Metric descriptors.
var (
	perfNDBSyncExcludedObjectsDescExcludedObjects = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "ndb_sync_excluded_objects_count"),
		"Number of NDB database objects which cannot be automatically synchronized between NDB Cluster's dictionary and the MySQL data dictionary.",
		nil, nil,
	)
)

// ScrapeReplicationGroupMembers collects from `performance_schema.ndb_sync_excluded_objects`.
type ScrapePerfNDBSyncExcludedObjects struct{}

// Name of the Scraper. Should be unique.
func (ScrapePerfNDBSyncExcludedObjects) Name() string {
	return performanceSchema + ".ndb_sync_excluded_objects"
}

// Help describes the role of the Scraper.
func (ScrapePerfNDBSyncExcludedObjects) Help() string {
	return "Collect metrics from performance_schema.ndb_sync_excluded_objects"
}

// Version of MySQL from which scraper is available.
func (ScrapePerfNDBSyncExcludedObjects) Version() float64 {
	return 8.0
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapePerfNDBSyncExcludedObjects) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, perfNDBSyncExcludedObjectsQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	var objectCount uint64
	for rows.Next() {
		if err := rows.Scan(&objectCount); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			perfNDBSyncExcludedObjectsDescExcludedObjects, prometheus.GaugeValue, float64(objectCount),
		)
	}

	return nil
}

// check interface
var _ Scraper = ScrapePerfNDBSyncExcludedObjects{}
