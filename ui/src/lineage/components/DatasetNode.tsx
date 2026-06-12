import { Handle, Position, type NodeProps } from 'reactflow'

import { roleToAccent } from '../graphUtils'
import type { DatasetNodeData } from '../types'

export function DatasetNode({ data }: NodeProps<DatasetNodeData>) {
  return (
    <div
      className={[
        'min-w-[260px] rounded-xl border border-slate-700 bg-slate-900/70 shadow-sm',
        'px-3 py-2 text-left',
        'border-l-4',
        roleToAccent(data.role),
      ].join(' ')}
      role="button"
      tabIndex={0}
      onClick={() => data.onSelectDataset(data.id)}
    >
      <Handle
        type="target"
        position={Position.Left}
        className="!h-2 !w-2 !border-2 !border-slate-200 !bg-slate-950"
      />
      <Handle
        type="source"
        position={Position.Right}
        className="!h-2 !w-2 !border-2 !border-slate-200 !bg-slate-950"
      />
      <div className="text-sm font-semibold text-slate-50">{data.label}</div>
      <div className="mt-0.5 text-xs text-slate-300">{data.columnsCount} columns</div>
      <div className="mt-2 flex flex-wrap gap-1.5">
        {data.columnsPreview.map((c) => (
          <button
            key={c}
            type="button"
            onClick={(e) => {
              e.stopPropagation()
              data.onSelectColumn(data.id, c)
            }}
            onContextMenu={(e) => data.onColumnContextMenu(e, data.id, c)}
            className={[
              'rounded-full px-2 py-0.5 text-[11px]',
              c === data.selectedColumn
                ? 'bg-sky-500 text-slate-950'
                : 'bg-slate-800 text-slate-200 hover:bg-slate-700',
            ].join(' ')}
          >
            {c}
          </button>
        ))}
      </div>
    </div>
  )
}
