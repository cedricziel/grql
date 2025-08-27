import { DataSourceInstanceSettings, CoreApp, ScopedVars } from '@grafana/data';
import { DataSourceWithBackend, getTemplateSrv } from '@grafana/runtime';

import { GrqlQuery, GrqlDataSourceOptions, DEFAULT_QUERY } from './types';

export class DataSource extends DataSourceWithBackend<GrqlQuery, GrqlDataSourceOptions> {
  constructor(instanceSettings: DataSourceInstanceSettings<GrqlDataSourceOptions>) {
    super(instanceSettings);
  }

  getDefaultQuery(_: CoreApp): Partial<GrqlQuery> {
    return DEFAULT_QUERY;
  }

  applyTemplateVariables(query: GrqlQuery, scopedVars: ScopedVars) {
    return {
      ...query,
      rawQuery: getTemplateSrv().replace(query.rawQuery, scopedVars),
    };
  }

  filterQuery(query: GrqlQuery): boolean {
    // if no query has been provided, prevent the query from being executed
    return !!query.rawQuery;
  }
}
