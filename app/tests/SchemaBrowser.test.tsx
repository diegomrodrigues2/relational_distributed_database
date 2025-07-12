import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import '@testing-library/jest-dom'
import { vi } from 'vitest'

vi.mock('../services/api', () => ({
  getTableList: vi.fn(),
  getTableSchema: vi.fn(),
  getTableStats: vi.fn(),
  getColumnStats: vi.fn(),
  analyzeTable: vi.fn(),
}))

import SchemaBrowser from '../components/SchemaBrowser'
import * as api from '../services/api'

describe('SchemaBrowser', () => {
  it('loads tables and shows schema details', async () => {
    ;(api.getTableList as any).mockResolvedValue(['users'])
    ;(api.getTableSchema as any).mockResolvedValue({
      name: 'users',
      columns: [{ name: 'id', data_type: 'int', primary_key: true }],
      indexes: [],
    })

    render(<SchemaBrowser />)
    await waitFor(() => {
      expect(api.getTableList).toHaveBeenCalled()
    })

    fireEvent.click(screen.getByText('users'))

    await waitFor(() => {
      expect(api.getTableSchema).toHaveBeenCalledWith('users')
    })

    expect(screen.getByText('id')).toBeInTheDocument()
    expect(screen.getByText('int')).toBeInTheDocument()
  })

  it('loads statistics on stats tab', async () => {
    ;(api.getTableList as any).mockResolvedValue(['users'])
    ;(api.getTableSchema as any).mockResolvedValue({
      name: 'users',
      columns: [{ name: 'id', data_type: 'int', primary_key: true }],
      indexes: [],
    })
    ;(api.getTableStats as any).mockResolvedValue({ table_name: 'users', num_rows: 3 })
    ;(api.getColumnStats as any).mockResolvedValue([
      { table_name: 'users', col_name: 'id', num_distinct: 3 },
    ])

    render(<SchemaBrowser />)
    fireEvent.click(await screen.findByText('users'))

    await waitFor(() => {
      expect(api.getTableSchema).toHaveBeenCalledWith('users')
    })

    fireEvent.click(screen.getByText('Estatísticas'))

    await waitFor(() => {
      expect(api.getTableStats).toHaveBeenCalledWith('users')
      expect(api.getColumnStats).toHaveBeenCalledWith('users')
    })

    expect(screen.getByText('Registros: 3')).toBeInTheDocument()
    expect(screen.getByText('id')).toBeInTheDocument()
  })
})
