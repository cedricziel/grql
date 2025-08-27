package models

import (
	"encoding/json"
	"fmt"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
)

type PluginSettings struct {
	Host               string                `json:"host"`
	Port               int                   `json:"port"`
	UseTLS             bool                  `json:"useTLS"`
	InsecureSkipVerify bool                  `json:"insecureSkipVerify"`
	Secrets            *SecretPluginSettings `json:"-"`
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
