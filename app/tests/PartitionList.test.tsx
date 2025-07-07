import { render, screen } from '@testing-library/react'
import '@testing-library/jest-dom'
import PartitionList from '../components/dashboard/PartitionList'
import { type Partition, type Node, NodeStatus } from '../types'

const partitions: Partition[] = [
  { id: 'p1', primaryNodeId: 'n1', replicaNodeIds: ['n2'], keyRange: ['a', 'z'], size: 1, itemCount: 100, operationCount: 0 },
]
const nodes: Node[] = [
  { id: 'n1', address: 'addr1', status: NodeStatus.LIVE, uptime: '1d', cpuUsage: 0, memoryUsage: 0, diskUsage: 0, dataLoad: 0, replicationLogSize: 0, hintsCount: 0 },
  { id: 'n2', address: 'addr2', status: NodeStatus.LIVE, uptime: '1d', cpuUsage: 0, memoryUsage: 0, diskUsage: 0, dataLoad: 0, replicationLogSize: 0, hintsCount: 0 },
]

describe('PartitionList', () => {
  it('renders partition table', () => {
    render(<PartitionList partitions={partitions} nodes={nodes} strategy="hash" />)
    expect(screen.getByText('Partitions')).toBeInTheDocument()
    expect(screen.getByText('p1')).toBeInTheDocument()
    expect(screen.getByText('hash')).toBeInTheDocument()
    expect(screen.getByText('n1 (addr1)')).toBeInTheDocument()
  })

  it('shows empty message', () => {
    render(<PartitionList partitions={[]} nodes={[]} strategy="hash" />)
    expect(screen.getByText('No partitions found.')).toBeInTheDocument()
  })
})
