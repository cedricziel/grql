import { DataSourceJsonData } from '@grafana/data';
import { DataQuery } from '@grafana/schema';

export interface GrqlQuery extends DataQuery {
  rawQuery: string;
  format?: 'table' | 'time_series';
}

export const DEFAULT_QUERY: Partial<GrqlQuery> = {
  rawQuery: 'SELECT * FROM metrics SINCE 1 hour ago',
  format: 'time_series',
};

/**
 * These are options configured for each DataSource instance
 */
export interface GrqlDataSourceOptions extends DataSourceJsonData {
  host?: string;
  port?: number;
  useTLS?: boolean;
  insecureSkipVerify?: boolean;
}

/**
 * Value that is used in the backend, but never sent over HTTP to the frontend
 */
export interface GrqlSecureJsonData {
  tlsCert?: string;
  tlsKey?: string;
  tlsCACert?: string;
}
