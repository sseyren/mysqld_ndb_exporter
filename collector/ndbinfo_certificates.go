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

// Scrape `ndbinfo.certificates`.

package collector

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const ndbinfoCertificatesQuery = `
	SELECT
		Node_id,
		Name,
		Expires,
		Serial
	FROM ndbinfo.certificates;
`

// Metric descriptors.
var (
	ndbinfoCertificatesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "certificate_expire_date"),
		"Expire date of certificates used by NDB nodes connecting with TLS link encryption as UNIX timestamp.",
		[]string{"node_id", "cert_name", "cert_serial"}, nil,
	)
)

// ScrapeNDBInfoCertificates collects from `ndbinfo.certificates`.
type ScrapeNDBInfoCertificates struct{}

// Name of the Scraper. Should be unique.
func (ScrapeNDBInfoCertificates) Name() string {
	return ndbInfo + ".certificates"
}

// Help describes the role of the Scraper.
func (ScrapeNDBInfoCertificates) Help() string {
	return "Collect metrics from ndbinfo.certificates"
}

// Version of MySQL from which scraper is available.
func (ScrapeNDBInfoCertificates) Version() float64 {
	return 8.4
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeNDBInfoCertificates) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, ndbinfoCertificatesQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		id, name, expires, serial string
	)
	for rows.Next() {
		if err := rows.Scan(&id, &name, &expires, &serial); err != nil {
			return err
		}
		expireDate, err := time.Parse("02-Jan-2006", expires)
		if err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			ndbinfoCertificatesDesc, prometheus.GaugeValue, float64(expireDate.Unix()),
			id, name, serial,
		)
	}

	return nil
}

// check interface
var _ Scraper = ScrapeNDBInfoCertificates{}
