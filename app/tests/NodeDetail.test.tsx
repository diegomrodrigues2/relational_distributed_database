import { render, screen, fireEvent } from '@testing-library/react'
import '@testing-library/jest-dom'
import { vi } from 'vitest'
import NodeDetail from '../components/management/NodeDetail'
import { NodeStatus, type Node } from '../types'

vi.mock('../components/management/StorageInspector', () => ({
  default: () => <div data-testid="inspector" />,
}))

const node: Node = {
  id: 'node1',
  address: 'localhost:9000',
  status: NodeStatus.SUSPECT,
  uptime: '1d',
  cpuUsage: 1,
  memoryUsage: 2,
  diskUsage: 3,
  dataLoad: 0,
  replicationLogSize: 0,
  hintsCount: 0,
}

describe('NodeDetail', () => {
  it('renders details and triggers callbacks', () => {
    const onRemove = vi.fn()
    const onStop = vi.fn()
    const onStart = vi.fn()
    render(
      <NodeDetail
        node={node}
        onRemove={onRemove}
        onStop={onStop}
        onStart={onStart}
        isLoading={false}
      />
    )

    expect(screen.getByText(`Node: ${node.id}`)).toBeInTheDocument()
    expect(screen.getByText(node.address)).toBeInTheDocument()
    expect(screen.getByText(node.status)).toBeInTheDocument()

    fireEvent.click(screen.getByText('Start'))
    expect(onStart).toHaveBeenCalledWith(node.id)

    fireEvent.click(screen.getByText('Stop'))
    expect(onStop).toHaveBeenCalledWith(node.id)

    fireEvent.click(screen.getByText('Remove Node'))
    expect(onRemove).toHaveBeenCalledWith(node.id)
  })
})
