import React, { ChangeEvent } from 'react';
import { InlineField, Select, Stack, CodeEditor } from '@grafana/ui';
import { QueryEditorProps, SelectableValue } from '@grafana/data';
import { DataSource } from '../datasource';
import { GrqlDataSourceOptions, GrqlQuery } from '../types';

type Props = QueryEditorProps<DataSource, GrqlQuery, GrqlDataSourceOptions>;

const formatOptions: Array<SelectableValue<string>> = [
  { label: 'Time series', value: 'time_series', description: 'For graph panels' },
  { label: 'Table', value: 'table', description: 'For table panels' },
];

export function QueryEditor({ query, onChange, onRunQuery }: Props) {
  const onQueryChange = (value: string) => {
    onChange({ ...query, rawQuery: value });
  };

  const onFormatChange = (value: SelectableValue<string>) => {
    onChange({ ...query, format: value.value as 'table' | 'time_series' });
    onRunQuery();
  };

  const onBlur = () => {
    onRunQuery();
  };

  const { rawQuery, format } = query;

  return (
    <Stack gap={1} direction="column">
      <InlineField label="Format" labelWidth={14} grow>
        <Select
          options={formatOptions}
          value={formatOptions.find((option) => option.value === format) || formatOptions[0]}
          onChange={onFormatChange}
        />
      </InlineField>
      
      <InlineField label="Query" labelWidth={14} grow>
        <CodeEditor
          height={200}
          language="sql"
          value={rawQuery || ''}
          onBlur={onBlur}
          onChange={onQueryChange}
          showMiniMap={false}
          showLineNumbers={true}
          monacoOptions={{
            folding: false,
            fontSize: 14,
            lineNumbers: 'on',
            wordWrap: 'on',
            minimap: {
              enabled: false,
            },
            scrollBeyondLastLine: false,
            overviewRulerLanes: 0,
          }}
        />
      </InlineField>
    </Stack>
  );
}