import { render, screen, fireEvent } from '@testing-library/react'
import '@testing-library/jest-dom'
import { vi } from 'vitest'
import NodeSelector from '../components/management/NodeSelector'
import { NodeStatus, type Node } from '../types'

const nodes: Node[] = [
  { id: 'n1', address: 'addr1', status: NodeStatus.LIVE, uptime: '1d', cpuUsage: 1, memoryUsage: 2, diskUsage: 3, dataLoad: 0, replicationLogSize: 0, hintsCount: 0 },
  { id: 'n2', address: 'addr2', status: NodeStatus.DEAD, uptime: '2d', cpuUsage: 1, memoryUsage: 2, diskUsage: 3, dataLoad: 0, replicationLogSize: 0, hintsCount: 0 },
]

describe('NodeSelector', () => {
  it('renders nodes and handles selection and add', () => {
    const onSelect = vi.fn()
    const onAdd = vi.fn()
    render(
      <NodeSelector
        nodes={nodes}
        selectedNodeId={null}
        onSelectNode={onSelect}
        onAddNode={onAdd}
        isActionLoading={false}
      />
    )

    expect(screen.getByText('Cluster Nodes')).toBeInTheDocument()
    expect(screen.getByText('n1')).toBeInTheDocument()
    expect(screen.getByText('n2')).toBeInTheDocument()

    fireEvent.click(screen.getByText('n2'))
    expect(onSelect).toHaveBeenCalledWith('n2')

    fireEvent.click(screen.getByText('Add New Node'))
    expect(onAdd).toHaveBeenCalled()
  })

  it('shows loading state when adding', () => {
    render(
      <NodeSelector
        nodes={nodes.slice(0, 1)}
        selectedNodeId={'n1'}
        onSelectNode={() => {}}
        onAddNode={() => {}}
        isActionLoading={true}
      />
    )

    const btn = screen.getByText('Adding...')
    expect(btn).toBeDisabled()
  })
})
