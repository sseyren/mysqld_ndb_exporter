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

// Scrape `ndbinfo.membership`.

package collector

import (
	"context"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

const ndbinfoMembershipQuery = `
	SELECT
		node_id,
		group_id,
		president,
		successor,
		arbitrator,
		CONV(arb_ticket, 16, 10) as arb_ticket_dec,
		arb_state,
		arb_connected
	FROM ndbinfo.membership;
`

// Metric descriptors.
var (
	ndbinfoMembershipDescPresident = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "membership_president"),
		"The ID of the other node that this node sees as president.",
		[]string{"node_id", "group_id"}, nil,
	)
	ndbinfoMembershipDescSuccessor = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "membership_successor"),
		"The ID of the other node that this node sees as successor of president.",
		[]string{"node_id", "group_id"}, nil,
	)
	ndbinfoMembershipDescArbitrator = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "membership_arbitrator"),
		"The ID of the other node that this node sees as arbitrator.",
		[]string{"node_id", "group_id"}, nil,
	)
	ndbinfoMembershipDescArbTicket = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "membership_arb_ticket"),
		"Internal identifier used to track arbitration of this node.",
		[]string{"node_id", "group_id"}, nil,
	)
	ndbinfoMembershipDescArbState = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "membership_arb_state"),
		"Arbitration state of this node. Values are; 0=NULL 1=INIT 2=FIND 3=PREP1 4=PREP2 5=START 6=RUN 7=CHOOSE 8=CRASH",
		[]string{"node_id", "group_id"}, nil,
	)
	ndbinfoMembershipDescArbConnected = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "membership_arb_connected"),
		"Whether this node is connected to the arbitrator node; either of 1 (yes) or 0 (no) or -1 (unknown).",
		[]string{"node_id", "group_id"}, nil,
	)
)

// ScrapeNDBInfoMembership collects from `ndbinfo.membership`.
type ScrapeNDBInfoMembership struct{}

// Name of the Scraper. Should be unique.
func (ScrapeNDBInfoMembership) Name() string {
	return ndbInfo + ".membership"
}

// Help describes the role of the Scraper.
func (ScrapeNDBInfoMembership) Help() string {
	return "Collect metrics from ndbinfo.membership"
}

// Version of MySQL from which scraper is available.
func (ScrapeNDBInfoMembership) Version() float64 {
	return 5.6
}

func ndbinfoMembershipArbStateToNumber(state string) float64 {
	switch state {
	case "ARBIT_NULL":
		return 0
	case "ARBIT_INIT":
		return 1
	case "ARBIT_FIND":
		return 2
	case "ARBIT_PREP1":
		return 3
	case "ARBIT_PREP2":
		return 4
	case "ARBIT_START":
		return 5
	case "ARBIT_RUN":
		return 6
	case "ARBIT_CHOOSE":
		return 7
	case "ARBIT_CRASH":
		return 8
	default:
		return -1
	}
}

func ndbinfoMembershipArbConnectedToNumber(state string) float64 {
	switch state {
	case "Yes":
		return 1
	case "No":
		return 0
	default:
		return -1
	}
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeNDBInfoMembership) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, ndbinfoMembershipQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		nodeID, groupID                                string
		president, successor, arbitrator, arbTicketDec uint64
		arbState, arbConnected                         string
	)
	for rows.Next() {
		if err := rows.Scan(
			&nodeID, &groupID, &president, &successor, &arbitrator, &arbTicketDec,
			&arbState, &arbConnected,
		); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			ndbinfoMembershipDescPresident, prometheus.GaugeValue, float64(president),
			nodeID, groupID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoMembershipDescSuccessor, prometheus.GaugeValue, float64(successor),
			nodeID, groupID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoMembershipDescArbitrator, prometheus.GaugeValue, float64(arbitrator),
			nodeID, groupID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoMembershipDescArbTicket, prometheus.GaugeValue, float64(arbTicketDec),
			nodeID, groupID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoMembershipDescArbState, prometheus.GaugeValue, ndbinfoMembershipArbStateToNumber(arbState),
			nodeID, groupID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoMembershipDescArbConnected, prometheus.GaugeValue, ndbinfoMembershipArbConnectedToNumber(arbConnected),
			nodeID, groupID,
		)
	}

	return nil
}

// check interface
var _ Scraper = ScrapeNDBInfoMembership{}
