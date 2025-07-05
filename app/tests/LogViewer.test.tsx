import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import '@testing-library/jest-dom'
import { vi } from 'vitest'

vi.mock('../services/api', () => ({
  getNodes: vi.fn(),
  getClusterEvents: vi.fn(),
  getNodeEvents: vi.fn(),
}))

import LogViewer from '../components/internals/LogViewer'
import { getNodes, getClusterEvents, getNodeEvents } from '../services/api'
import { NodeStatus, type Node } from '../types'

const nodes: Node[] = [
  { id: 'n1', address: 'a', status: NodeStatus.LIVE, uptime: '1d', cpuUsage: 0, memoryUsage: 0, diskUsage: 0, dataLoad: 0, replicationLogSize: 0, hintsCount: 0 },
]

beforeEach(() => {
  vi.resetAllMocks()
  ;(getNodes as any).mockResolvedValue(nodes)
  ;(getClusterEvents as any).mockResolvedValue(['c1'])
  ;(getNodeEvents as any).mockResolvedValue(['n1e'])
})

describe('LogViewer', () => {
  it('fetches and displays logs for cluster and node', async () => {
    render(<LogViewer />)

    await waitFor(() => {
      expect(getNodes).toHaveBeenCalled()
    })
    await waitFor(() => {
      expect(getClusterEvents).toHaveBeenCalled()
    })
    expect(screen.getByText('c1')).toBeInTheDocument()

    fireEvent.change(screen.getByRole('combobox'), { target: { value: 'n1' } })
    await waitFor(() => {
      expect(getNodeEvents).toHaveBeenCalledWith('n1')
    })
    expect(screen.getByText('n1e')).toBeInTheDocument()
  })
})
