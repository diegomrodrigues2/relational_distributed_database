import { render, screen } from '@testing-library/react'
import '@testing-library/jest-dom'
import NodeList from '../components/dashboard/NodeList'
import { NodeStatus, type Node } from '../types'

const nodes: Node[] = [
  { id: 'node_1', address: 'localhost:9000', status: NodeStatus.LIVE, uptime: '1d', cpuUsage: 1, memoryUsage: 2, diskUsage: 3, dataLoad: 0, replicationLogSize: 0, hintsCount: 0 }
]

describe('NodeList', () => {
  it('renders node table', () => {
    render(<NodeList nodes={nodes} onManageNode={() => {}} />)
    expect(screen.getByText('Nodes')).toBeInTheDocument()
    expect(screen.getByText('node_1')).toBeInTheDocument()
  })
})
