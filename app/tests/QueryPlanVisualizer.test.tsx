import { render, screen } from '@testing-library/react'
import '@testing-library/jest-dom'
import QueryPlanVisualizer, { PlanNode } from '../components/QueryPlanVisualizer'

const plan: PlanNode = {
  node_type: 'NestedLoopJoin',
  children: [
    { node_type: 'SeqScan', table: 'a' },
    { node_type: 'IndexScan', table: 'b', index: 'idx' },
  ],
}

describe('QueryPlanVisualizer', () => {
  it('renders plan tree', () => {
    render(<QueryPlanVisualizer plan={plan} />)
    expect(screen.getByText('NestedLoopJoin')).toBeInTheDocument()
    expect(screen.getAllByText('SeqScan')[0]).toBeInTheDocument()
    expect(screen.getAllByText('IndexScan')[0]).toBeInTheDocument()
  })
})
