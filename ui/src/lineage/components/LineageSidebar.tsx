import type { LineageMode, LineageResponse } from '../types'

type Props = {
  data: LineageResponse | null
  mode: LineageMode
  selectedDataset: string | null
  setSelectedDataset: (datasetId: string | null) => void
  selectedColumn: string | null
  setSelectedColumn: (column: string | null) => void
  deps: string[]
  dfSelectedDfId: string | null
  dfDeps: string[]
  dfExpr: unknown
  openContextMenu: (x: number, y: number, datasetId: string, column: string) => void
}

export function LineageSidebar({
  data,
  mode,
  selectedDataset,
  setSelectedDataset,
  selectedColumn,
  setSelectedColumn,
  deps,
  dfSelectedDfId,
  dfDeps,
  dfExpr,
  openContextMenu,
}: Props) {
  return (
    <div className="w-[360px] border-l border-slate-800 bg-slate-950 p-4">
      <div className="text-sm font-semibold text-slate-50">Column lineage</div>
      <div className="mt-1 text-xs text-slate-300">
        Click a dataset node to browse columns; click a column to highlight origins. Right-click a
        column for more actions.
      </div>

      {selectedDataset ? (
        <div className="mt-4">
          <div className="text-xs font-semibold text-slate-200">{selectedDataset}</div>
          <div className="mt-2 flex flex-wrap gap-2">
            {(data?.datasets?.[selectedDataset]?.columns ?? []).map((c) => (
              <button
                key={c}
                type="button"
                onClick={() => setSelectedColumn(c)}
                onContextMenu={(e) => {
                  e.preventDefault()
                  setSelectedDataset(selectedDataset)
                  setSelectedColumn(c)
                  openContextMenu(e.clientX, e.clientY, selectedDataset, c)
                }}
                className={[
                  'rounded-full px-2 py-1 text-[11px]',
                  c === selectedColumn
                    ? 'bg-sky-500 text-slate-950'
                    : 'bg-slate-800 text-slate-200 hover:bg-slate-700',
                ].join(' ')}
              >
                {c}
              </button>
            ))}
          </div>

          {selectedColumn ? (
            <div className="mt-4">
              <div className="text-xs font-semibold text-slate-200">Deps</div>
              <div className="mt-2 flex flex-wrap gap-2">
                {deps.length ? (
                  deps.map((d) => (
                    <span
                      key={d}
                      className="rounded-full bg-slate-900 px-2 py-1 text-[11px] text-slate-200 ring-1 ring-slate-700"
                    >
                      {d}
                    </span>
                  ))
                ) : (
                  <span className="text-xs text-slate-400">(none)</span>
                )}
              </div>
              {mode === 'dataframe' ? (
                <div className="mt-4">
                  <div className="text-xs font-semibold text-slate-200">Dataframe details</div>
                  <div className="mt-1 text-xs text-slate-300">
                    Transform:{' '}
                    <span className="font-medium text-slate-100">
                      {data?.datasets?.[selectedDataset]?.producer?.filename ?? '(unknown)'}
                    </span>
                  </div>
                  <div className="mt-1 text-xs text-slate-300">
                    {dfSelectedDfId ? `Resolved in ${dfSelectedDfId}` : 'No df entry found'}
                  </div>
                  <div className="mt-2 text-[11px] text-slate-300">
                    Deps: {dfDeps.length ? dfDeps.join(', ') : '(none)'}
                  </div>
                  <pre className="mt-2 max-h-40 overflow-auto rounded-md bg-slate-900 p-2 text-[10px] text-slate-200 ring-1 ring-slate-800">
                    {JSON.stringify(dfExpr, null, 2)}
                  </pre>
                </div>
              ) : null}
            </div>
          ) : null}

          <div className="mt-4">
            <button
              type="button"
              onClick={() => {
                setSelectedDataset(null)
                setSelectedColumn(null)
              }}
              className="rounded-md bg-slate-800 px-3 py-1.5 text-xs text-slate-100 hover:bg-slate-700"
            >
              Clear selection
            </button>
          </div>
        </div>
      ) : (
        <div className="mt-4 text-xs text-slate-400">No dataset selected.</div>
      )}
    </div>
  )
}
