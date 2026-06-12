import type React from 'react'

export type LineageDataset = {
  columns: string[]
  graph: { ops?: unknown[] } & Record<string, unknown>
  producer?: {
    id: string
    name: string
    module?: string | null
    file?: string | null
    filename?: string | null
  }
}

export type LineageTransform = {
  id: string
  name: string
  module?: string | null
  inputs: string[]
  outputs: string[]
}

export type LineageResponse = {
  datasets: Record<string, LineageDataset>
  transforms?: LineageTransform[]
}

export type DatasetRole = 'source' | 'intermediate' | 'sink'

export type DatasetNodeData = {
  id: string
  label: string
  role: DatasetRole
  columnsCount: number
  columnsPreview: string[]
  onSelectDataset: (datasetId: string) => void
  onSelectColumn: (datasetId: string, column: string) => void
  onColumnContextMenu: (e: React.MouseEvent, datasetId: string, column: string) => void
  selectedColumn?: string | null
}

export type LayoutDirection = 'LR' | 'TB'

export type LineageMode = 'dataset' | 'dataframe' | 'column'

export type ContextMenuState =
  | {
      open: true
      x: number
      y: number
      datasetId: string
      column: string
    }
  | { open: false }

export type DfOp = {
  op: string
  in: string[]
  out: string
}

export type StubGraph = {
  ops?: unknown
  dataframes?: unknown
}

export type StubDataframes = Record<
  string,
  {
    id?: string
    columns?: Record<string, { deps?: unknown; expr?: unknown }>
  }
>
