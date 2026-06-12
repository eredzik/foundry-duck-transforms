type Props = {
  open: boolean
  x: number
  y: number
  datasetId: string
  column: string
  onClose: () => void
  onShowDataframe: () => void
  onShowColumn: () => void
}

export function ColumnContextMenu({
  open,
  x,
  y,
  datasetId,
  column,
  onClose,
  onShowDataframe,
  onShowColumn,
}: Props) {
  if (!open) return null

  return (
    <div
      className="fixed z-50 w-72 rounded-md border border-slate-700 bg-slate-950 p-1 shadow-xl"
      style={{ left: x, top: y }}
      onClick={(e) => e.stopPropagation()}
    >
      <div className="px-2 py-1 text-[11px] text-slate-300">
        {datasetId} / <span className="text-sky-300">{column}</span>
      </div>
      <button
        type="button"
        className="w-full rounded px-2 py-2 text-left text-xs text-slate-100 hover:bg-slate-800"
        onClick={() => {
          onShowDataframe()
          onClose()
        }}
      >
        Show dataframe level lineage for this column
      </button>
      <button
        type="button"
        className="w-full rounded px-2 py-2 text-left text-xs text-slate-100 hover:bg-slate-800"
        onClick={() => {
          onShowColumn()
          onClose()
        }}
      >
        Show column level lineage for this column
      </button>
    </div>
  )
}
