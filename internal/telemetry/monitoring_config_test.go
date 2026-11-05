package telemetry

import (
	"os"
	"testing"

	"go.yaml.in/yaml/v2"
)

func TestProductionPrometheusRoutesAlerts(t *testing.T) {
	data, err := os.ReadFile("../../deployments/security/prometheus.yml")
	if err != nil {
		t.Fatal(err)
	}
	var config struct {
		Alerting struct {
			Alertmanagers []struct {
				Static []struct {
					Targets []string `yaml:"targets"`
				} `yaml:"static_configs"`
			} `yaml:"alertmanagers"`
		} `yaml:"alerting"`
	}
	if err := yaml.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}
	for _, manager := range config.Alerting.Alertmanagers {
		for _, group := range manager.Static {
			for _, target := range group.Targets {
				if target == "alertmanager:9093" {
					return
				}
			}
		}
	}
	t.Fatal("production Prometheus does not route to Alertmanager")
}
