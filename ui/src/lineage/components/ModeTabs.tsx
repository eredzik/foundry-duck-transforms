import type { LineageMode } from '../types'

type Props = {
  mode: LineageMode
  setMode: (mode: LineageMode) => void
  canDrillDown: boolean
}

export function ModeTabs({ mode, setMode, canDrillDown }: Props) {
  return (
    <div className="flex items-center gap-2">
      <button
        type="button"
        onClick={() => setMode('dataset')}
        className={[
          'rounded-md px-2 py-1 text-xs',
          mode === 'dataset' ? 'bg-slate-200 text-slate-950' : 'bg-slate-800 text-slate-200 hover:bg-slate-700',
        ].join(' ')}
      >
        Dataset
      </button>
      <button
        type="button"
        onClick={() => setMode('dataframe')}
        disabled={!canDrillDown}
        className={[
          'rounded-md px-2 py-1 text-xs',
          !canDrillDown
            ? 'bg-slate-900 text-slate-500 cursor-not-allowed'
            : mode === 'dataframe'
              ? 'bg-slate-200 text-slate-950'
              : 'bg-slate-800 text-slate-200 hover:bg-slate-700',
        ].join(' ')}
      >
        Dataframes
      </button>
      <button
        type="button"
        onClick={() => setMode('column')}
        disabled={!canDrillDown}
        className={[
          'rounded-md px-2 py-1 text-xs',
          !canDrillDown
            ? 'bg-slate-900 text-slate-500 cursor-not-allowed'
            : mode === 'column'
              ? 'bg-slate-200 text-slate-950'
              : 'bg-slate-800 text-slate-200 hover:bg-slate-700',
        ].join(' ')}
      >
        Column
      </button>
    </div>
  )
}
