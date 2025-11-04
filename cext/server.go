// SPDX-FileCopyrightText: 2023 Rivos Inc.
//
// SPDX-License-Identifier: Apache-2.0
package cext

import (
	"net/http"
	"os"

	"log/slog"

	"github.com/TusharMohapatra07/prometheus-slurm-exporter/exporter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func InitPromServer(config *exporter.Config) (http.Handler, []Destructor) {
	textHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: config.LogLevel,
	})
	slog.SetDefault(slog.New(textHandler))
	nodeCollector := exporter.NewNodeCollecter(config)
	cNodeFetcher := NewNodeFetcher(config.PollLimit)
	nodeCollector.SetFetcher(cNodeFetcher)
	prometheus.MustRegister(nodeCollector)
	CJobFetcher := NewJobFetcher(config.PollLimit)
	jobCollector := exporter.NewJobsController(config)
	jobCollector.SetFetcher(CJobFetcher)
	prometheus.MustRegister(jobCollector)
	return promhttp.Handler(), []Destructor{cNodeFetcher, CJobFetcher}
}
