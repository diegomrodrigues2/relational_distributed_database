import { render, screen } from '@testing-library/react'
import '@testing-library/jest-dom'
import PartitionList from '../components/dashboard/PartitionList'
import { type Partition } from '../types'

const partitions: Partition[] = [
  { id: 'p1', primaryNodeId: 'n1', replicaNodeIds: ['n2'], keyRange: ['a', 'z'], size: 1, itemCount: 100, operationCount: 0 },
]

describe('PartitionList', () => {
  it('renders partition table', () => {
    render(<PartitionList partitions={partitions} />)
    expect(screen.getByText('Partitions')).toBeInTheDocument()
    expect(screen.getByText('p1')).toBeInTheDocument()
  })

  it('shows empty message', () => {
    render(<PartitionList partitions={[]} />)
    expect(screen.getByText('No partitions found.')).toBeInTheDocument()
  })
})
