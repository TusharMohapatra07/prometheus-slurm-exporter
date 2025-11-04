package exporter

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

type GpuMetrics struct {
	Alloc       float64
	Idle        float64
	Total       float64
	Utilization float64
}

// GPU response structures for JSON API
type sinfoGpuNode struct {
	Gres string `json:"gres"`
}

type sinfoGpuResponse struct {
	Meta struct {
		SlurmVersion struct {
			Version struct {
				Major int `json:"major"`
				Micro int `json:"micro"`
				Minor int `json:"minor"`
			} `json:"version"`
			Release string `json:"release"`
		} `json:"Slurm"`
	} `json:"meta"`
	Errors []string       `json:"errors"`
	Nodes  []sinfoGpuNode `json:"nodes"`
}

type sacctGpuJob struct {
	AllocGRES string `json:"allocated_gres"`
}

type sacctGpuResponse struct {
	Meta struct {
		SlurmVersion struct {
			Version struct {
				Major int `json:"major"`
				Micro int `json:"micro"`
				Minor int `json:"minor"`
			} `json:"version"`
			Release string `json:"release"`
		} `json:"Slurm"`
	} `json:"meta"`
	Errors []string      `json:"errors"`
	Jobs   []sacctGpuJob `json:"jobs"`
}

type GpuJsonFetcher struct {
	sinfoScraper SlurmByteScraper
	sacctScraper SlurmByteScraper
	errorCounter prometheus.Counter
	cache        *gpuCache
}

type gpuCache struct {
	sync.Mutex
	t        time.Time
	limit    float64
	cache    *GpuMetrics
	duration time.Duration
}

func (gmf *GpuJsonFetcher) fetch() (*GpuMetrics, error) {
	totalGpus, err := gmf.fetchTotalGpus()
	if err != nil {
		return nil, err
	}

	allocGpus, err := gmf.fetchAllocatedGpus()
	if err != nil {
		return nil, err
	}

	idleGpus := totalGpus - allocGpus
	utilization := 0.0
	if totalGpus > 0 {
		utilization = allocGpus / totalGpus
	}

	metrics := &GpuMetrics{
		Alloc:       allocGpus,
		Idle:        idleGpus,
		Total:       totalGpus,
		Utilization: utilization,
	}

	return metrics, nil
}

func (gmf *GpuJsonFetcher) fetchTotalGpus() (float64, error) {
	sinfoResp := new(sinfoGpuResponse)
	cliJson, err := gmf.sinfoScraper.FetchRawBytes()
	if err != nil {
		return 0, err
	}

	if err := json.Unmarshal(cliJson, sinfoResp); err != nil {
		slog.Error(fmt.Sprintf("Unmarshaling sinfo GPU metrics: %q", err))
		return 0, err
	}

	if len(sinfoResp.Errors) > 0 {
		for _, e := range sinfoResp.Errors {
			slog.Error(fmt.Sprintf("sinfo API error response: %q", e))
		}
		gmf.errorCounter.Add(float64(len(sinfoResp.Errors)))
		return 0, errors.New(sinfoResp.Errors[0])
	}

	totalGpus := 0.0
	for _, node := range sinfoResp.Nodes {
		gpuCount := parseGresGpuCount(node.Gres)
		totalGpus += gpuCount
	}

	return totalGpus, nil
}

func (gmf *GpuJsonFetcher) fetchAllocatedGpus() (float64, error) {
	sacctResp := new(sacctGpuResponse)
	cliJson, err := gmf.sacctScraper.FetchRawBytes()
	if err != nil {
		return 0, err
	}

	if err := json.Unmarshal(cliJson, sacctResp); err != nil {
		slog.Error(fmt.Sprintf("Unmarshaling sacct GPU metrics: %q", err))
		return 0, err
	}

	if len(sacctResp.Errors) > 0 {
		for _, e := range sacctResp.Errors {
			slog.Error(fmt.Sprintf("sacct API error response: %q", e))
		}
		gmf.errorCounter.Add(float64(len(sacctResp.Errors)))
		return 0, errors.New(sacctResp.Errors[0])
	}

	allocGpus := 0.0
	for _, job := range sacctResp.Jobs {
		gpuCount := parseGresGpuCount(job.AllocGRES)
		allocGpus += gpuCount
	}

	return allocGpus, nil
}

func (gmf *GpuJsonFetcher) FetchMetrics() (*GpuMetrics, error) {
	gmf.cache.Lock()
	defer gmf.cache.Unlock()

	if gmf.cache.cache != nil && time.Since(gmf.cache.t).Seconds() < gmf.cache.limit {
		return gmf.cache.cache, nil
	}

	t := time.Now()
	metrics, err := gmf.fetch()
	if err != nil {
		return nil, err
	}
	gmf.cache.duration = time.Since(t)
	gmf.cache.cache = metrics
	gmf.cache.t = time.Now()
	return metrics, nil
}

func (gmf *GpuJsonFetcher) ScrapeError() prometheus.Counter {
	return gmf.errorCounter
}

func (gmf *GpuJsonFetcher) ScrapeDuration() time.Duration {
	return gmf.sinfoScraper.Duration()
}

// CLI Fallback Fetcher
type GpuCliFallbackFetcher struct {
	sinfoScraper SlurmByteScraper
	sacctScraper SlurmByteScraper
	errorCounter prometheus.Counter
	cache        *gpuCache
}

func (gcf *GpuCliFallbackFetcher) fetch() (*GpuMetrics, error) {
	totalGpus, err := gcf.fetchTotalGpus()
	if err != nil {
		return nil, err
	}

	allocGpus, err := gcf.fetchAllocatedGpus()
	if err != nil {
		return nil, err
	}

	idleGpus := totalGpus - allocGpus
	utilization := 0.0
	if totalGpus > 0 {
		utilization = allocGpus / totalGpus
	}

	metrics := &GpuMetrics{
		Alloc:       allocGpus,
		Idle:        idleGpus,
		Total:       totalGpus,
		Utilization: utilization,
	}

	return metrics, nil
}

func (gcf *GpuCliFallbackFetcher) fetchTotalGpus() (float64, error) {
	sinfoOutput, err := gcf.sinfoScraper.FetchRawBytes()
	if err != nil {
		return 0, err
	}

	sinfoOutput = bytes.TrimSpace(sinfoOutput)
	if len(sinfoOutput) == 0 {
		return 0, nil
	}

	totalGpus := 0.0
	reader := csv.NewReader(bytes.NewReader(sinfoOutput))
	reader.Comma = '|'
	reader.LazyQuotes = true

	records, err := reader.ReadAll()
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to parse sinfo GPU output: %q", err))
		gcf.errorCounter.Inc()
		return 0, err
	}

	for _, record := range records {
		if len(record) < 2 {
			continue
		}
		gresField := strings.TrimSpace(record[1])
		gpuCount := parseGresGpuCount(gresField)
		totalGpus += gpuCount
	}

	return totalGpus, nil
}

func (gcf *GpuCliFallbackFetcher) fetchAllocatedGpus() (float64, error) {
	sacctOutput, err := gcf.sacctScraper.FetchRawBytes()
	if err != nil {
		return 0, err
	}

	sacctOutput = bytes.TrimSpace(sacctOutput)
	if len(sacctOutput) == 0 {
		return 0, nil
	}

	allocGpus := 0.0
	for _, line := range bytes.Split(sacctOutput, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		gresField := strings.Trim(string(line), "\"")
		gpuCount := parseGresGpuCount(gresField)
		allocGpus += gpuCount
	}

	return allocGpus, nil
}

func (gcf *GpuCliFallbackFetcher) FetchMetrics() (*GpuMetrics, error) {
	gcf.cache.Lock()
	defer gcf.cache.Unlock()

	if gcf.cache.cache != nil && time.Since(gcf.cache.t).Seconds() < gcf.cache.limit {
		return gcf.cache.cache, nil
	}

	t := time.Now()
	metrics, err := gcf.fetch()
	if err != nil {
		return nil, err
	}
	gcf.cache.duration = time.Since(t)
	gcf.cache.cache = metrics
	gcf.cache.t = time.Now()
	return metrics, nil
}

func (gcf *GpuCliFallbackFetcher) ScrapeError() prometheus.Counter {
	return gcf.errorCounter
}

func (gcf *GpuCliFallbackFetcher) ScrapeDuration() time.Duration {
	return gcf.sinfoScraper.Duration()
}

// parseGresGpuCount parses GPU count from GRES or TRES string
// GRES Examples: "gpu:2", "gpu:tesla:2", "gpu:1(IDX:0)"
// TRES Examples: "cpu=4,mem=1024M,gres/gpu=2", "billing=8,cpu=8,gres/gpu=4,mem=32G,node=1"
func parseGresGpuCount(gres string) float64 {
	if gres == "" || gres == "N/A" || gres == "(null)" {
		return 0
	}

	// Handle TRES format: "cpu=4,mem=1024M,gres/gpu=2"
	if strings.Contains(gres, "=") {
		// This is TRES format
		for _, part := range strings.Split(gres, ",") {
			part = strings.TrimSpace(part)
			// Look for gres/gpu=N or gres/gpu:type=N
			if strings.HasPrefix(part, "gres/gpu") {
				// Extract the value after =
				eqIdx := strings.Index(part, "=")
				if eqIdx != -1 && eqIdx < len(part)-1 {
					countStr := part[eqIdx+1:]
					// Remove any type suffix like gres/gpu:tesla=4
					if count, err := strconv.ParseFloat(countStr, 64); err == nil {
						return count
					}
				}
			}
		}
		return 0 // No GPU found in TRES
	}

	// Handle multiple GRES resources separated by comma (legacy GRES format)
	if strings.Contains(gres, ",") {
		total := 0.0
		for _, part := range strings.Split(gres, ",") {
			total += parseGresGpuCount(strings.TrimSpace(part))
		}
		return total
	}

	// Remove any trailing index information like (IDX:0-1)
	if idx := strings.Index(gres, "("); idx != -1 {
		gres = gres[:idx]
	}

	// Check if it contains "gpu"
	if !strings.Contains(strings.ToLower(gres), "gpu") {
		return 0
	}

	// Split by colon to get parts: ["gpu", "type", "count"] or ["gpu", "count"]
	parts := strings.Split(gres, ":")
	if len(parts) < 2 {
		return 0
	}

	// The last part should be the count
	countStr := parts[len(parts)-1]
	count, err := strconv.ParseFloat(countStr, 64)
	if err != nil {
		slog.Debug(fmt.Sprintf("Failed to parse GPU count from '%s': %v", gres, err))
		return 0
	}

	return count
}

type GpuFetcher interface {
	FetchMetrics() (*GpuMetrics, error)
	ScrapeError() prometheus.Counter
	ScrapeDuration() time.Duration
}

// GpuCollector implements the Prometheus Collector interface
type GpuCollector struct {
	alloc       *prometheus.Desc
	idle        *prometheus.Desc
	total       *prometheus.Desc
	utilization *prometheus.Desc
	fetcher     GpuFetcher
}

func NewGpuCollector(config *Config) *GpuCollector {
	var fetcher GpuFetcher
	cliOpts := config.cliOpts

	if cliOpts.fallback {
		// CLI fallback mode
		fetcher = &GpuCliFallbackFetcher{
			sinfoScraper: NewCliScraper(cliOpts.sinfoGpu...),
			sacctScraper: NewCliScraper(cliOpts.sacctGpu...),
			cache: &gpuCache{
				limit: config.PollLimit,
			},
			errorCounter: prometheus.NewCounter(prometheus.CounterOpts{
				Name: "gpu_scrape_errors",
				Help: "GPU scrape errors",
			}),
		}
	} else {
		// JSON API mode
		fetcher = &GpuJsonFetcher{
			sinfoScraper: NewCliScraper(cliOpts.sinfoGpu...),
			sacctScraper: NewCliScraper(cliOpts.sacctGpu...),
			cache: &gpuCache{
				limit: config.PollLimit,
			},
			errorCounter: prometheus.NewCounter(prometheus.CounterOpts{
				Name: "gpu_scrape_errors",
				Help: "GPU scrape errors",
			}),
		}
	}

	return &GpuCollector{
		alloc: prometheus.NewDesc(
			"slurm_gpus_alloc",
			"Allocated GPUs",
			nil,
			nil,
		),
		idle: prometheus.NewDesc(
			"slurm_gpus_idle",
			"Idle GPUs",
			nil,
			nil,
		),
		total: prometheus.NewDesc(
			"slurm_gpus_total",
			"Total GPUs",
			nil,
			nil,
		),
		utilization: prometheus.NewDesc(
			"slurm_gpus_utilization",
			"Total GPU utilization",
			nil,
			nil,
		),
		fetcher: fetcher,
	}
}

func (gc *GpuCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- gc.alloc
	ch <- gc.idle
	ch <- gc.total
	ch <- gc.utilization
}

func (gc *GpuCollector) Collect(ch chan<- prometheus.Metric) {
	metrics, err := gc.fetcher.FetchMetrics()
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to fetch GPU metrics: %q", err))
		return
	}

	if metrics == nil {
		return
	}

	ch <- prometheus.MustNewConstMetric(gc.alloc, prometheus.GaugeValue, metrics.Alloc)
	ch <- prometheus.MustNewConstMetric(gc.idle, prometheus.GaugeValue, metrics.Idle)
	ch <- prometheus.MustNewConstMetric(gc.total, prometheus.GaugeValue, metrics.Total)
	ch <- prometheus.MustNewConstMetric(gc.utilization, prometheus.GaugeValue, metrics.Utilization)
}
