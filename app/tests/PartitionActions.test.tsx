import { vi } from 'vitest'

vi.mock('../services/api', () => ({
  splitPartition: vi.fn(),
  mergePartitions: vi.fn(),
}))

import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import '@testing-library/jest-dom'
import PartitionActions from '../components/management/PartitionActions'
import { splitPartition, mergePartitions } from '../services/api'

describe('PartitionActions', () => {
  it('calls splitPartition when Split is clicked', async () => {
    render(<PartitionActions />)
    fireEvent.change(screen.getByPlaceholderText('Partition ID'), { target: { value: '3' } })
    fireEvent.change(screen.getByPlaceholderText('Split Key (optional)'), { target: { value: 'k' } })
    fireEvent.click(screen.getByText('Split'))
    await waitFor(() => {
      expect(splitPartition).toHaveBeenCalledWith(3, 'k')
    })
  })

  it('calls mergePartitions when Merge is clicked', async () => {
    render(<PartitionActions />)
    fireEvent.change(screen.getByPlaceholderText('Left PID'), { target: { value: '1' } })
    fireEvent.change(screen.getByPlaceholderText('Right PID'), { target: { value: '2' } })
    fireEvent.click(screen.getByText('Merge'))
    await waitFor(() => {
      expect(mergePartitions).toHaveBeenCalledWith(1, 2)
    })
  })
})
