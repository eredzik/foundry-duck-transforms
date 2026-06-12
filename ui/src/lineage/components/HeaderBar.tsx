import { ModeTabs } from './ModeTabs'
import type { LineageMode, LineageResponse } from '../types'

type Props = {
  mode: LineageMode
  setMode: (mode: LineageMode) => void
  selectedDataset: string | null
  selectedColumn: string | null
  data: LineageResponse | null
  error: string | null
  nodesCount: number
}

function getModeLabel(mode: LineageMode): string {
  switch (mode) {
    case 'dataset':
      return 'Dataset graph'
    case 'dataframe':
      return 'Dataframe graph'
    default:
      return 'Column DAG'
  }
}

export function HeaderBar({
  mode,
  setMode,
  selectedDataset,
  selectedColumn,
  data,
  error,
  nodesCount,
}: Props) {
  const canDrillDown = Boolean(selectedDataset && selectedColumn)

  return (
    <div className="flex h-14 items-center gap-3 border-b border-slate-800 bg-slate-950 px-4">
      <div className="text-sm font-semibold text-slate-50">Lineage</div>
      <div className="text-xs text-slate-300">{getModeLabel(mode)}</div>
      <ModeTabs mode={mode} setMode={setMode} canDrillDown={canDrillDown} />
      {selectedDataset ? (
        <div className="text-xs text-slate-300">
          <span className="text-slate-200">Selected</span> {selectedDataset}
          {selectedColumn ? (
            <>
              <span className="mx-1 text-slate-500">/</span>
              <span className="text-sky-300">{selectedColumn}</span>
            </>
          ) : null}
        </div>
      ) : null}
      <div className="ml-auto text-xs text-slate-300">
        {error
          ? `Error: ${error}`
          : data
            ? `${Object.keys(data.datasets ?? {}).length} datasets (df-level) • ${nodesCount} nodes`
            : 'Loading…'}
      </div>
    </div>
  )
}
