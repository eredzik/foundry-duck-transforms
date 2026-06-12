import type React from 'react'
import dagre from 'dagre'
import { MarkerType, type Edge, type Node } from 'reactflow'

import type {
  DatasetNodeData,
  DatasetRole,
  DfOp,
  LayoutDirection,
  LineageResponse,
  StubDataframes,
  StubGraph,
} from './types'

export function roleToAccent(role: DatasetRole): string {
  switch (role) {
    case 'source':
      return 'border-emerald-400/80'
    case 'sink':
      return 'border-fuchsia-400/80'
    default:
      return 'border-sky-400/80'
  }
}

export function layoutDagre(nodes: Node[], edges: Edge[], direction: LayoutDirection): Node[] {
  const g = new dagre.graphlib.Graph()
  g.setDefaultEdgeLabel(() => ({}))
  g.setGraph({
    rankdir: direction,
    // Keep generous spacing to avoid visual collisions in dense DAGs.
    nodesep: 110,
    ranksep: 150,
    edgesep: 48,
    marginx: 32,
    marginy: 32,
  })

  const defaultWidth = 280
  const defaultHeight = 100

  const estimateNodeSize = (n: Node): { width: number; height: number } => {
    // Prefer measured sizes from reactflow when available.
    if (typeof n.width === 'number' && typeof n.height === 'number') {
      return { width: n.width, height: n.height }
    }

    if (n.type === 'dataset') {
      return { width: 320, height: 150 }
    }

    // Default nodes use multiline text labels. Estimate line count so dagre
    // can reserve enough vertical room and prevent overlap.
    const label = (n.data as { label?: unknown } | undefined)?.label
    const labelText = typeof label === 'string' ? label : ''
    const lines = labelText ? labelText.split('\n').length : 1
    const width = Math.max(300, labelText.length > 60 ? 360 : 300)
    const height = 92 + Math.max(0, lines - 1) * 18
    return { width, height }
  }

  for (const n of nodes) {
    const { width, height } = estimateNodeSize(n)
    g.setNode(n.id, { width, height })
  }
  for (const e of edges) g.setEdge(e.source, e.target)

  dagre.layout(g)

  return nodes.map((n) => {
    const nodeWithPosition = g.node(n.id) as { x: number; y: number; width?: number; height?: number } | undefined
    const width = nodeWithPosition?.width ?? defaultWidth
    const height = nodeWithPosition?.height ?? defaultHeight
    return {
      ...n,
      position: {
        x: (nodeWithPosition?.x ?? 0) - width / 2,
        y: (nodeWithPosition?.y ?? 0) - height / 2,
      },
    }
  })
}

function isRecord(v: unknown): v is Record<string, unknown> {
  return Boolean(v) && typeof v === 'object' && !Array.isArray(v)
}

function asStubGraph(graph: unknown): StubGraph | null {
  if (!isRecord(graph)) return null
  return graph as StubGraph
}

function asStubDataframes(dfs: unknown): StubDataframes | null {
  if (!isRecord(dfs)) return null
  return dfs as StubDataframes
}

export function coerceDfOps(graph: unknown): DfOp[] {
  const g = asStubGraph(graph)
  if (!g || !Array.isArray(g.ops)) return []
  return (g.ops as unknown[])
    .map((o) => (isRecord(o) ? o : {}))
    .map((o) => ({
      op: String(o.op ?? ''),
      in: Array.isArray(o.in) ? (o.in as unknown[]).map((x) => String(x)) : [],
      out: String(o.out ?? ''),
    }))
    .filter((o: DfOp) => o.out)
}

export function getDfColumns(graph: unknown, dfId: string): string[] {
  const g = asStubGraph(graph)
  if (!g) return []
  const dfs = asStubDataframes(g.dataframes)
  if (!dfs) return []
  const cols = dfs[dfId]?.columns
  if (!cols) return []
  return Object.keys(cols)
}

export function getDfColumnEntry(graph: unknown, dfId: string, column: string): { deps: string[]; expr: unknown } | null {
  const g = asStubGraph(graph)
  if (!g) return null
  const dfs = asStubDataframes(g.dataframes)
  if (!dfs) return null
  const entry = dfs[dfId]?.columns?.[column]
  if (!entry) return null
  const deps = Array.isArray(entry.deps)
    ? (entry.deps as unknown[]).filter((d): d is string => typeof d === 'string')
    : []
  return { deps, expr: entry.expr }
}

export function getDfColumnDeps(graph: unknown, dfId: string, column: string): string[] {
  return getDfColumnEntry(graph, dfId, column)?.deps ?? []
}

export function getColumnDeps(graph: unknown, column: string): string[] {
  if (!graph || typeof graph !== 'object') return []
  const g = graph as Record<string, unknown>
  const dfs = g['dataframes']
  const dfMap =
    dfs && typeof dfs === 'object' && !Array.isArray(dfs) ? (dfs as Record<string, unknown>) : null
  if (!dfMap) return []

  const dfIds = Object.keys(dfMap).sort((a, b) => {
    const na = Number(a.replace('df_', ''))
    const nb = Number(b.replace('df_', ''))
    return (Number.isFinite(na) ? na : 0) - (Number.isFinite(nb) ? nb : 0)
  })

  for (let i = dfIds.length - 1; i >= 0; i--) {
    const df = dfMap[dfIds[i]] as unknown
    if (!isRecord(df)) continue
    const cols = (df as Record<string, unknown>)['columns']
    if (!isRecord(cols)) continue
    const entry = (cols as Record<string, unknown>)[column]
    if (!isRecord(entry)) continue
    const deps = (entry as Record<string, unknown>)['deps']
    if (Array.isArray(deps)) return deps.filter((d: unknown) => typeof d === 'string') as string[]
  }
  return []
}

export function buildDatasetGraph(args: {
  data: LineageResponse | null
  selectedDataset: string | null
  selectedColumn: string | null
  onSelectDataset: (datasetId: string) => void
  onSelectColumn: (datasetId: string, column: string) => void
  onColumnContextMenu: (e: React.MouseEvent, datasetId: string, column: string) => void
}): {
  datasetNodes: Node<DatasetNodeData>[]
  datasetEdges: Edge[]
  highlightNodes: Set<string>
  highlightEdges: Set<string>
  deps: string[]
} {
  const { data, selectedDataset, selectedColumn, onSelectDataset, onSelectColumn, onColumnContextMenu } = args
  const ds = data?.datasets ?? {}
  const transforms = data?.transforms ?? []

  const edges: Edge[] = []
  const allNodeIds = new Set<string>()
  const inputIds = new Set<string>()
  const outputIds = new Set<string>()

  const rev: Record<string, string[]> = {}

  for (const t of transforms) {
    for (const input of t.inputs ?? []) {
      allNodeIds.add(input)
      inputIds.add(input)
    }
    for (const output of t.outputs ?? []) {
      allNodeIds.add(output)
      outputIds.add(output)
    }
  }

  for (const id of Object.keys(ds)) allNodeIds.add(id)

  for (const t of transforms) {
    for (const input of t.inputs ?? []) {
      for (const output of t.outputs ?? []) {
        edges.push({
          id: `${input}->${output}`,
          source: input,
          target: output,
          type: 'smoothstep',
          markerEnd: { type: MarkerType.ArrowClosed },
          style: { strokeWidth: 2, stroke: '#e2e8f0' },
        })
        rev[output] = rev[output] || []
        rev[output].push(input)
      }
    }
  }

  const ids = Array.from(allNodeIds).sort()
  const nodes: Node<DatasetNodeData>[] = ids.map((id, idx) => {
    const isInputOnly = inputIds.has(id) && !outputIds.has(id)
    const isOutputOnly = outputIds.has(id) && !inputIds.has(id)
    const role: DatasetRole = isInputOnly ? 'source' : isOutputOnly ? 'sink' : 'intermediate'

    const cols = ds[id]?.columns ?? []
    const preview = cols.slice(0, 8)
    return {
      id,
      type: 'dataset',
      position: { x: (idx % 3) * 360, y: Math.floor(idx / 3) * 220 },
      data: {
        id,
        label: id,
        role,
        columnsCount: cols.length,
        columnsPreview: preview,
        onSelectDataset,
        onSelectColumn,
        onColumnContextMenu,
        selectedColumn: selectedDataset === id ? selectedColumn : null,
      },
    }
  })

  const highlightNodes = new Set<string>()
  const highlightEdges = new Set<string>()
  const deps: string[] = []

  if (selectedDataset && selectedColumn) {
    const graph = ds[selectedDataset]?.graph
    deps.push(...getColumnDeps(graph, selectedColumn))

    const upstream = new Set<string>()
    const q: string[] = [selectedDataset]
    const seen = new Set<string>(q)
    while (q.length) {
      const cur = q.shift()!
      upstream.add(cur)
      for (const p of rev[cur] || []) {
        if (!seen.has(p)) {
          seen.add(p)
          q.push(p)
        }
      }
    }

    const originCandidates = Array.from(upstream).filter((dId) => {
      const cols = ds[dId]?.columns ?? []
      return deps.some((dep) => cols.includes(dep))
    })

    highlightNodes.add(selectedDataset)
    for (const origin of originCandidates) highlightNodes.add(origin)
    for (const u of upstream) highlightNodes.add(u)

    for (const e of edges) {
      if (highlightNodes.has(e.source) && highlightNodes.has(e.target)) {
        highlightEdges.add(e.id)
      }
    }
  }

  const laidOut = layoutDagre(nodes, edges, 'LR')
  return { datasetNodes: laidOut, datasetEdges: edges, highlightNodes, highlightEdges, deps }
}

export function buildDataframeGraph(args: {
  data: LineageResponse | null
  selectedDataset: string | null
  selectedColumn: string | null
}): {
  dfNodes: Node[]
  dfEdges: Edge[]
  dfSelectedDfId: string | null
  dfDeps: string[]
  dfExpr: unknown
} {
  const { data, selectedDataset, selectedColumn } = args
  const ds = data?.datasets ?? {}
  if (!selectedDataset || !selectedColumn) {
    return { dfNodes: [], dfEdges: [], dfSelectedDfId: null, dfDeps: [], dfExpr: null }
  }
  const graph = ds[selectedDataset]?.graph
  const ops = coerceDfOps(graph)
  const producerFilename = ds[selectedDataset]?.producer?.filename ?? null
  const opByOut = new Map<string, string>()
  for (const op of ops) {
    if (!opByOut.has(op.out) && op.op) opByOut.set(op.out, op.op)
  }
  const dfIds = new Set<string>()
  for (const op of ops) {
    for (const i of op.in) dfIds.add(i)
    dfIds.add(op.out)
  }
  const ids = Array.from(dfIds).sort((a, b) => {
    const na = Number(a.replace('df_', ''))
    const nb = Number(b.replace('df_', ''))
    return (Number.isFinite(na) ? na : 0) - (Number.isFinite(nb) ? nb : 0)
  })
  const nodes: Node[] = ids.map((id, idx) => {
    const cols = getDfColumns(graph, id)
    const entry = getDfColumnEntry(graph, id, selectedColumn)
    const isSelectedDf = Boolean(entry)
    const opLabel = opByOut.get(id) ?? ''
    const titleLeft = producerFilename ? `${producerFilename} · ${id}` : id
    return {
      id,
      type: 'default',
      position: { x: (idx % 4) * 260, y: Math.floor(idx / 4) * 160 },
      data: {
        label: [
          titleLeft,
          opLabel ? `op: ${opLabel}` : '',
          `${cols.length} cols`,
          isSelectedDf ? `has "${selectedColumn}"` : '',
        ]
          .filter(Boolean)
          .join('\n'),
      },
      style: {
        border: isSelectedDf ? '2px solid #38bdf8' : '1px solid rgba(100,116,139,.7)',
        borderLeft: isSelectedDf ? '4px solid #38bdf8' : '4px solid rgba(56,189,248,.8)',
        background: 'rgba(15,23,42,.7)',
        borderRadius: 12,
        boxShadow: '0 1px 2px rgba(0,0,0,.25)',
        padding: 10,
        opacity: 1,
        whiteSpace: 'pre-line',
        color: '#f8fafc',
      },
    }
  })
  const edges: Edge[] = []
  for (const op of ops) {
    for (const src of op.in) {
      edges.push({
        id: `${src}->${op.out}:${op.op}`,
        source: src,
        target: op.out,
        type: 'smoothstep',
        markerEnd: { type: MarkerType.ArrowClosed },
        // Keep dataframe edge visuals consistent with dataset mode defaults.
        style: { strokeWidth: 2, stroke: '#e2e8f0' },
        label: op.op,
        labelStyle: { fill: '#cbd5e1', fontSize: 10 },
      })
    }
  }

  let selectedDf: string | null = null
  let deps: string[] = []
  let expr: unknown = null
  for (let i = ids.length - 1; i >= 0; i--) {
    const entry = getDfColumnEntry(graph, ids[i], selectedColumn)
    if (entry) {
      selectedDf = ids[i]
      deps = entry.deps
      expr = entry.expr
      break
    }
  }
  const laidOut = layoutDagre(nodes, edges, 'LR')
  return { dfNodes: laidOut, dfEdges: edges, dfSelectedDfId: selectedDf, dfDeps: deps, dfExpr: expr }
}

export function buildColumnGraph(args: {
  data: LineageResponse | null
  selectedDataset: string | null
  selectedColumn: string | null
}): {
  colNodes: Node[]
  colEdges: Edge[]
  colHighlightedEdges: Set<string>
} {
  const { data, selectedDataset, selectedColumn } = args
  const ds = data?.datasets ?? {}
  if (!selectedDataset || !selectedColumn) {
    return { colNodes: [], colEdges: [], colHighlightedEdges: new Set<string>() }
  }
  const graph = ds[selectedDataset]?.graph
  const ops = coerceDfOps(graph)
  const dfIds = new Set<string>()
  for (const op of ops) {
    for (const i of op.in) dfIds.add(i)
    dfIds.add(op.out)
  }
  const orderedDfIds = Array.from(dfIds).sort((a, b) => {
    const na = Number(a.replace('df_', ''))
    const nb = Number(b.replace('df_', ''))
    return (Number.isFinite(na) ? na : 0) - (Number.isFinite(nb) ? nb : 0)
  })
  const dfIndex = new Map<string, number>(orderedDfIds.map((id, i) => [id, i]))

  let startDf: string | null = null
  for (let i = orderedDfIds.length - 1; i >= 0; i--) {
    if (getDfColumnEntry(graph, orderedDfIds[i], selectedColumn)) {
      startDf = orderedDfIds[i]
      break
    }
  }
  if (!startDf) return { colNodes: [], colEdges: [], colHighlightedEdges: new Set<string>() }

  const findOriginDf = (currentDf: string, depCol: string): string | null => {
    const curIdx = dfIndex.get(currentDf) ?? orderedDfIds.length - 1
    for (let i = curIdx; i >= 0; i--) {
      const dfId = orderedDfIds[i]
      if (getDfColumnEntry(graph, dfId, depCol)) return dfId
    }
    return null
  }

  type ColKey = { dfId: string; col: string }

  const nodesMap = new Map<string, ColKey>()
  const edges: Edge[] = []
  const visiting = new Set<string>()
  const visited = new Set<string>()

  const addNode = (dfId: string, col: string) => {
    const id = `${dfId}.${col}`
    if (!nodesMap.has(id)) nodesMap.set(id, { dfId, col })
    return id
  }

  const walk = (dfId: string, col: string) => {
    const id = addNode(dfId, col)
    if (visited.has(id) || visiting.has(id)) return
    visiting.add(id)

    const deps = getDfColumnDeps(graph, dfId, col)
    for (const dep of deps) {
      const originDf = findOriginDf(dfId, dep) ?? dfId
      const depId = addNode(originDf, dep)
      edges.push({
        id: `${depId}->${id}`,
        source: depId,
        target: id,
        type: 'smoothstep',
        markerEnd: { type: MarkerType.ArrowClosed },
        style: { strokeWidth: 2, stroke: '#94a3b8' },
      })
      walk(originDf, dep)
    }

    visiting.delete(id)
    visited.add(id)
  }

  walk(startDf, selectedColumn)

  const colNodes: Node[] = Array.from(nodesMap.entries())
    .sort((a, b) => {
      const ai = dfIndex.get(a[1].dfId) ?? 0
      const bi = dfIndex.get(b[1].dfId) ?? 0
      if (ai !== bi) return ai - bi
      return a[1].col.localeCompare(b[1].col)
    })
    .map(([id, k], idx) => {
      const entry = getDfColumnEntry(graph, k.dfId, k.col)
      const deps = entry?.deps ?? []
      const isTarget = k.dfId === startDf && k.col === selectedColumn
      return {
        id,
        position: { x: (idx % 4) * 260, y: Math.floor(idx / 4) * 150 },
        data: {
          label: [k.col, k.dfId, `deps: ${deps.length ? deps.join(', ') : '(none)'}`].join('\n'),
        },
        style: {
          border: isTarget ? '2px solid #38bdf8' : '1px solid rgba(100,116,139,.7)',
          borderLeft: isTarget ? '4px solid #38bdf8' : '4px solid rgba(56,189,248,.8)',
          background: 'rgba(15,23,42,.7)',
          borderRadius: 12,
          boxShadow: '0 1px 2px rgba(0,0,0,.25)',
          padding: 10,
          whiteSpace: 'pre-line',
          color: '#f8fafc',
        },
      }
    })

  const highlightEdges = new Set<string>(edges.map((e) => e.id))
  const laidOut = layoutDagre(colNodes, edges, 'LR')
  return { colNodes: laidOut, colEdges: edges, colHighlightedEdges: highlightEdges }
}
