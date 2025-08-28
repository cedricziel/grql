package models

import (
	"encoding/json"
	"fmt"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
)

type PluginSettings struct {
	Secrets            *SecretPluginSettings `json:"-"`
	Host               string                `json:"host"`
	Port               int                   `json:"port"`
	InsecureSkipVerify bool                  `json:"insecureSkipVerify"`
	UseTLS             bool                  `json:"useTLS"`
}

type SecretPluginSettings struct {
	TLSCert   string `json:"tlsCert"`
	TLSKey    string `json:"tlsKey"`
	TLSCACert string `json:"tlsCACert"`
}

func LoadPluginSettings(source backend.DataSourceInstanceSettings) (*PluginSettings, error) {
	settings := PluginSettings{
		Host: "localhost",
		Port: 50051,
	}

	if len(source.JSONData) > 0 {
		err := json.Unmarshal(source.JSONData, &settings)
		if err != nil {
			return nil, fmt.Errorf("could not unmarshal PluginSettings json: %w", err)
		}
	}

	settings.Secrets = loadSecretPluginSettings(source.DecryptedSecureJSONData)

	return &settings, nil
}

func loadSecretPluginSettings(source map[string]string) *SecretPluginSettings {
	return &SecretPluginSettings{
		TLSCert:   source["tlsCert"],
		TLSKey:    source["tlsKey"],
		TLSCACert: source["tlsCACert"],
	}
}
