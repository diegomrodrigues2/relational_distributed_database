import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import '@testing-library/jest-dom'
import { vi } from 'vitest'

vi.mock('../services/databaseService', () => ({
  getUserRecords: vi.fn(),
  saveUserRecord: vi.fn(),
  deleteUserRecord: vi.fn(),
}))

import DataBrowser from '../components/DataBrowser'
import * as databaseService from '../services/databaseService'

const page1Records = Array.from({ length: 20 }, (_, i) => ({
  partitionKey: `p${i}`,
  clusteringKey: `c${i}`,
  value: `{"a":${i}}`,
}))
const page2Records = [
  { partitionKey: 'p20', clusteringKey: 'c20', value: '{"a":20}' },
]

beforeEach(() => {
  vi.resetAllMocks()
  ;(databaseService.getUserRecords as any).mockResolvedValue(page1Records)
  ;(databaseService.saveUserRecord as any).mockResolvedValue(page1Records[0])
  ;(databaseService.deleteUserRecord as any).mockResolvedValue(undefined)
})

describe('DataBrowser', () => {
  it('loads records on mount and filters search', async () => {
    render(<DataBrowser />)
    await waitFor(() => {
      expect(databaseService.getUserRecords).toHaveBeenCalledWith(0, 20)
    })
    expect(screen.getByText('p1')).toBeInTheDocument()
    expect(screen.getByText('p2')).toBeInTheDocument()

    fireEvent.change(screen.getByPlaceholderText('Search by key or value...'), { target: { value: 'p2' } })
    expect(screen.queryByText('p1')).not.toBeInTheDocument()
    expect(screen.getByText('p2')).toBeInTheDocument()
  })

  it('creates a record via the service', async () => {
    render(<DataBrowser />)
    await screen.findByText('Create New Record')
    fireEvent.click(screen.getByText('Create New Record'))

    fireEvent.change(screen.getByLabelText('Partition Key'), { target: { value: 'p3' } })
    fireEvent.change(screen.getByLabelText('Clustering Key'), { target: { value: 'c3' } })
    fireEvent.change(screen.getByLabelText('Value'), { target: { value: '{"b":3}' } })

    fireEvent.click(screen.getByText('Create Record'))
    await waitFor(() => {
      expect(databaseService.saveUserRecord).toHaveBeenCalledWith({ partitionKey: 'p3', clusteringKey: 'c3', value: '{"b":3}' })
    })
    expect(databaseService.getUserRecords).toHaveBeenCalledTimes(2)
    expect((databaseService.getUserRecords as any).mock.calls[1]).toEqual([0, 20])
  })

  it('deletes a record via the service', async () => {
    vi.spyOn(window, 'confirm').mockReturnValue(true)
    render(<DataBrowser />)
    await screen.findAllByText('Delete')

    fireEvent.click(screen.getAllByText('Delete')[0])
    await waitFor(() => {
      expect(databaseService.deleteUserRecord).toHaveBeenCalledWith('p0', 'c0')
    })
    expect(databaseService.getUserRecords).toHaveBeenCalledTimes(2)
    expect((databaseService.getUserRecords as any).mock.calls[1]).toEqual([0, 20])
  })

  it('navigates pages to load different record slices', async () => {
    ;(databaseService.getUserRecords as any).mockResolvedValueOnce(page1Records)
    ;(databaseService.getUserRecords as any).mockResolvedValueOnce(page2Records)

    render(<DataBrowser />)
    await screen.findByText('p1')

    fireEvent.click(screen.getByText('Next'))

    await waitFor(() => {
      expect(databaseService.getUserRecords).toHaveBeenLastCalledWith(20, 20)
    })

    expect(screen.queryByText('p0')).not.toBeInTheDocument()
    expect(screen.getByText('p20')).toBeInTheDocument()
  })
})
