import ReactFlow, { Background, Controls, MiniMap, MarkerType, type Edge, type Node } from 'reactflow'

import type { DatasetNodeData, LineageMode } from '../types'
import { DatasetNode } from './DatasetNode'

type Props = {
  mode: LineageMode
  nodes: Node[]
  edges: Edge[]
  selectedDataset: string | null
  selectedColumn: string | null
  highlightNodes: Set<string>
  highlightEdges: Set<string>
  colHighlightedEdges: Set<string>
}

export function LineageGraphCanvas({
  mode,
  nodes,
  edges,
  selectedDataset,
  selectedColumn,
  highlightNodes,
  highlightEdges,
  colHighlightedEdges,
}: Props) {
  const flowNodes: Node[] =
    mode === 'dataset'
      ? nodes.map((n) => {
          const isHighlighting = Boolean(selectedDataset && selectedColumn)
          const highlighted = highlightNodes.has(n.id)
          return {
            ...n,
            style: {
              ...(n.style || {}),
              opacity: isHighlighting ? (highlighted ? 1 : 0.18) : 1,
            },
          }
        })
      : nodes

  const flowEdges: Edge[] =
    mode === 'dataset'
      ? edges.map((e) => {
          const isHighlighting = Boolean(selectedDataset && selectedColumn)
          const highlighted = highlightEdges.has(e.id)
          return {
            ...e,
            animated: isHighlighting ? highlighted : false,
            style: {
              ...(e.style || {}),
              opacity: isHighlighting ? (highlighted ? 1 : 0.08) : 1,
              stroke: isHighlighting ? (highlighted ? '#38bdf8' : '#64748b') : '#e2e8f0',
              strokeWidth: isHighlighting ? (highlighted ? 3 : 2) : 2,
            },
          }
        })
      : mode === 'column'
        ? edges.map((e) => {
            const isTargeting = colHighlightedEdges.has(e.id)
            return {
              ...e,
              animated: isTargeting,
              style: {
                ...(e.style || {}),
                stroke: isTargeting ? '#38bdf8' : '#e2e8f0',
                strokeWidth: isTargeting ? 3 : 2,
              },
            }
          })
        : edges

  return (
    <ReactFlow
      nodes={flowNodes}
      edges={flowEdges}
      fitView
      nodesConnectable={false}
      nodeTypes={{ dataset: DatasetNode }}
      defaultEdgeOptions={{
        animated: false,
        style: { stroke: '#e2e8f0' },
        markerEnd: { type: MarkerType.ArrowClosed },
      }}
    >
      <MiniMap
        pannable
        nodeStrokeColor={(n) => {
          const role = (n.data as DatasetNodeData | undefined)?.role
          if (role === 'source') return '#34d399'
          if (role === 'sink') return '#e879f9'
          return '#38bdf8'
        }}
        nodeColor={() => '#0f172a'}
      />
      <Controls />
      <Background gap={24} color="#334155" />
    </ReactFlow>
  )
}
