import { useEffect, useMemo, useState } from 'react'
import 'reactflow/dist/style.css'
import { ColumnContextMenu } from './lineage/components/ColumnContextMenu'
import { HeaderBar } from './lineage/components/HeaderBar'
import { LineageGraphCanvas } from './lineage/components/LineageGraphCanvas'
import { LineageSidebar } from './lineage/components/LineageSidebar'
import { buildColumnGraph, buildDataframeGraph, buildDatasetGraph } from './lineage/graphUtils'
import type { ContextMenuState, LineageMode, LineageResponse } from './lineage/types'

function App() {
  const [data, setData] = useState<LineageResponse | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [selectedDataset, setSelectedDataset] = useState<string | null>(null)
  const [selectedColumn, setSelectedColumn] = useState<string | null>(null)
  const [mode, setMode] = useState<LineageMode>('dataset')
  const [contextMenu, setContextMenu] = useState<ContextMenuState>({ open: false })

  useEffect(() => {
    fetch('/api/lineage')
      .then(async (r) => {
        if (!r.ok) throw new Error(await r.text())
        return (await r.json()) as LineageResponse
      })
      .then((json) => setData(json))
      .catch((e: unknown) => setError(e instanceof Error ? e.message : String(e)))
  }, [])

  const { datasetNodes, datasetEdges, highlightNodes, highlightEdges, deps } = useMemo(
    () =>
      buildDatasetGraph({
        data,
        selectedDataset,
        selectedColumn,
        onSelectDataset: (datasetId) => {
          setSelectedDataset(datasetId)
          setSelectedColumn(null)
        },
        onSelectColumn: (datasetId, column) => {
          setSelectedDataset(datasetId)
          setSelectedColumn(column)
        },
        onColumnContextMenu: (e, datasetId, column) => {
          e.preventDefault()
          e.stopPropagation()
          setSelectedDataset(datasetId)
          setSelectedColumn(column)
          setContextMenu({ open: true, x: e.clientX, y: e.clientY, datasetId, column })
        },
      }),
    [data, selectedDataset, selectedColumn],
  )

  const { dfNodes, dfEdges, dfSelectedDfId, dfDeps, dfExpr } = useMemo(
    () => buildDataframeGraph({ data, selectedDataset, selectedColumn }),
    [data, selectedDataset, selectedColumn],
  )

  const { colNodes, colEdges, colHighlightedEdges } = useMemo(
    () => buildColumnGraph({ data, selectedDataset, selectedColumn }),
    [data, selectedDataset, selectedColumn],
  )

  const nodes = mode === 'dataframe' ? dfNodes : mode === 'column' ? colNodes : datasetNodes
  const edges = mode === 'dataframe' ? dfEdges : mode === 'column' ? colEdges : datasetEdges

  return (
    <div
      className="h-full w-full"
      onClick={() => {
        if (contextMenu.open) setContextMenu({ open: false })
      }}
    >
      <HeaderBar
        mode={mode}
        setMode={setMode}
        selectedDataset={selectedDataset}
        selectedColumn={selectedColumn}
        data={data}
        error={error}
        nodesCount={nodes.length}
      />

      <div className="flex h-[calc(100%-56px)]">
        <div className="flex-1">
          <LineageGraphCanvas
            mode={mode}
            nodes={nodes}
            edges={edges}
            selectedDataset={selectedDataset}
            selectedColumn={selectedColumn}
            highlightNodes={highlightNodes}
            highlightEdges={highlightEdges}
            colHighlightedEdges={colHighlightedEdges}
          />
        </div>

        <LineageSidebar
          data={data}
          mode={mode}
          selectedDataset={selectedDataset}
          setSelectedDataset={setSelectedDataset}
          selectedColumn={selectedColumn}
          setSelectedColumn={setSelectedColumn}
          deps={deps}
          dfSelectedDfId={dfSelectedDfId}
          dfDeps={dfDeps}
          dfExpr={dfExpr}
          openContextMenu={(x, y, datasetId, column) =>
            setContextMenu({ open: true, x, y, datasetId, column })
          }
        />
      </div>

      <ColumnContextMenu
        open={contextMenu.open}
        x={contextMenu.open ? contextMenu.x : 0}
        y={contextMenu.open ? contextMenu.y : 0}
        datasetId={contextMenu.open ? contextMenu.datasetId : ''}
        column={contextMenu.open ? contextMenu.column : ''}
        onClose={() => setContextMenu({ open: false })}
        onShowDataframe={() => {
          if (!contextMenu.open) return
          setSelectedDataset(contextMenu.datasetId)
          setSelectedColumn(contextMenu.column)
          setMode('dataframe')
        }}
        onShowColumn={() => {
          if (!contextMenu.open) return
          setSelectedDataset(contextMenu.datasetId)
          setSelectedColumn(contextMenu.column)
          setMode('column')
        }}
      />
    </div>
  )
}

export default App
