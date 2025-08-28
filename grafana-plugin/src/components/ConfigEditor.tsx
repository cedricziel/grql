import React, { ChangeEvent } from 'react';
import { InlineField, Input, SecretInput, InlineSwitch } from '@grafana/ui';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { GrqlDataSourceOptions, GrqlSecureJsonData } from '../types';

interface Props extends DataSourcePluginOptionsEditorProps<GrqlDataSourceOptions, GrqlSecureJsonData> {}

export function ConfigEditor(props: Props) {
  const { onOptionsChange, options } = props;
  const { jsonData, secureJsonFields, secureJsonData } = options;

  const onHostChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      jsonData: {
        ...jsonData,
        host: event.target.value,
      },
    });
  };

  const onPortChange = (event: ChangeEvent<HTMLInputElement>) => {
    const port = parseInt(event.target.value, 10);
    onOptionsChange({
      ...options,
      jsonData: {
        ...jsonData,
        port: isNaN(port) ? undefined : port,
      },
    });
  };

  const onUseTLSChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      jsonData: {
        ...jsonData,
        useTLS: event.target.checked,
      },
    });
  };

  const onSkipVerifyChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      jsonData: {
        ...jsonData,
        insecureSkipVerify: event.target.checked,
      },
    });
  };

  const onTLSCertChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      secureJsonData: {
        ...secureJsonData,
        tlsCert: event.target.value,
      },
    });
  };

  const onTLSKeyChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      secureJsonData: {
        ...secureJsonData,
        tlsKey: event.target.value,
      },
    });
  };

  const onTLSCACertChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      secureJsonData: {
        ...secureJsonData,
        tlsCACert: event.target.value,
      },
    });
  };

  const onResetTLSCert = () => {
    onOptionsChange({
      ...options,
      secureJsonFields: {
        ...options.secureJsonFields,
        tlsCert: false,
      },
      secureJsonData: {
        ...options.secureJsonData,
        tlsCert: '',
      },
    });
  };

  const onResetTLSKey = () => {
    onOptionsChange({
      ...options,
      secureJsonFields: {
        ...options.secureJsonFields,
        tlsKey: false,
      },
      secureJsonData: {
        ...options.secureJsonData,
        tlsKey: '',
      },
    });
  };

  const onResetTLSCACert = () => {
    onOptionsChange({
      ...options,
      secureJsonFields: {
        ...options.secureJsonFields,
        tlsCACert: false,
      },
      secureJsonData: {
        ...options.secureJsonData,
        tlsCACert: '',
      },
    });
  };

  return (
    <>
      <h3 className="page-heading">Connection</h3>
      
      <InlineField label="Host" labelWidth={14} interactive tooltip="gRQL server hostname or IP address">
        <Input
          id="config-editor-host"
          onChange={onHostChange}
          value={jsonData.host || ''}
          placeholder="localhost"
          width={40}
        />
      </InlineField>
      
      <InlineField label="Port" labelWidth={14} interactive tooltip="gRQL server port">
        <Input
          id="config-editor-port"
          onChange={onPortChange}
          value={jsonData.port || ''}
          placeholder="50051"
          width={40}
          type="number"
        />
      </InlineField>

      <h3 className="page-heading">Security</h3>

      <InlineField label="Use TLS" labelWidth={14} interactive tooltip="Enable TLS connection to gRQL server">
        <InlineSwitch
          id="config-editor-use-tls"
          onChange={onUseTLSChange}
          value={jsonData.useTLS || false}
        />
      </InlineField>

      {jsonData.useTLS && (
        <>
          <InlineField 
            label="Skip TLS Verify" 
            labelWidth={14} 
            interactive 
            tooltip="Skip TLS certificate verification (not recommended for production)"
          >
            <InlineSwitch
              id="config-editor-skip-verify"
              onChange={onSkipVerifyChange}
              value={jsonData.insecureSkipVerify || false}
            />
          </InlineField>

          <InlineField label="TLS Certificate" labelWidth={14} interactive tooltip="Client TLS certificate (PEM format)">
            <SecretInput
              id="config-editor-tls-cert"
              isConfigured={secureJsonFields?.tlsCert || false}
              value={secureJsonData?.tlsCert || ''}
              placeholder="-----BEGIN CERTIFICATE-----"
              width={40}
              onReset={onResetTLSCert}
              onChange={onTLSCertChange}
            />
          </InlineField>

          <InlineField label="TLS Key" labelWidth={14} interactive tooltip="Client TLS key (PEM format)">
            <SecretInput
              id="config-editor-tls-key"
              isConfigured={secureJsonFields?.tlsKey || false}
              value={secureJsonData?.tlsKey || ''}
              placeholder="-----BEGIN PRIVATE KEY-----"
              width={40}
              onReset={onResetTLSKey}
              onChange={onTLSKeyChange}
            />
          </InlineField>

          <InlineField label="TLS CA Certificate" labelWidth={14} interactive tooltip="CA certificate (PEM format)">
            <SecretInput
              id="config-editor-tls-ca-cert"
              isConfigured={secureJsonFields?.tlsCACert || false}
              value={secureJsonData?.tlsCACert || ''}
              placeholder="-----BEGIN CERTIFICATE-----"
              width={40}
              onReset={onResetTLSCACert}
              onChange={onTLSCACertChange}
            />
          </InlineField>
        </>
      )}
    </>
  );
}
