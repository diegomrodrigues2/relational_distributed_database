import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import '@testing-library/jest-dom'
import { vi } from 'vitest'

vi.mock('../services/api', () => ({
  getWalEntries: vi.fn(),
  getMemtableEntries: vi.fn(),
}))

import { WALView, MemTableView } from '../components/management/StorageInspector'
import { getWalEntries, getMemtableEntries } from '../services/api'

const pageItems = Array.from({ length: 20 }, (_, i) => ({
  type: 'PUT',
  key: `k${i}`,
  value: `v${i}`,
  vectorClock: {},
}))

const memItems = Array.from({ length: 20 }, (_, i) => ({
  key: `k${i}`,
  value: `v${i}`,
  vectorClock: {},
}))

beforeEach(() => {
  vi.resetAllMocks()
  ;(getWalEntries as any).mockResolvedValue(pageItems)
  ;(getMemtableEntries as any).mockResolvedValue(memItems)
})

describe('WALView pagination', () => {
  it('requests next and previous pages', async () => {
    render(<WALView nodeId="n1" />)
    await waitFor(() => {
      expect(getWalEntries).toHaveBeenCalledWith('n1', 0, 20)
    })

    ;(getWalEntries as any).mockResolvedValue([])
    fireEvent.click(screen.getByText('Next'))
    await waitFor(() => {
      expect(getWalEntries).toHaveBeenLastCalledWith('n1', 20, 20)
    })

    ;(getWalEntries as any).mockResolvedValue([])
    fireEvent.click(screen.getByText('Prev'))
    await waitFor(() => {
      expect(getWalEntries).toHaveBeenLastCalledWith('n1', 0, 20)
    })
  })
})

describe('MemTableView pagination', () => {
  it('requests next page', async () => {
    render(<MemTableView nodeId="n1" />)
    await waitFor(() => {
      expect(getMemtableEntries).toHaveBeenCalledWith('n1', 0, 20)
    })

    ;(getMemtableEntries as any).mockResolvedValue([])
    fireEvent.click(screen.getByText('Next'))
    await waitFor(() => {
      expect(getMemtableEntries).toHaveBeenLastCalledWith('n1', 20, 20)
    })
  })
})
