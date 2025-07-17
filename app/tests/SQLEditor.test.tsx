import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import '@testing-library/jest-dom'
import { vi } from 'vitest'

vi.mock('../services/api', () => ({
  runSqlQuery: vi.fn(),
  executeSql: vi.fn(),
}))

vi.mock('@uiw/react-codemirror', () => ({
  default: (props: any) => (
    <textarea data-testid="cm" value={props.value} onChange={e => props.onChange(e.target.value)} />
  ),
}))

import SQLEditor from '../components/SQLEditor'
import { runSqlQuery } from '../services/api'

describe('SQLEditor', () => {
  it('submits query and renders results', async () => {
    ;(runSqlQuery as any).mockResolvedValue({
      columns: [{ name: 'id', type: 'int' }],
      rows: [{ id: 1 }],
    })
    render(<SQLEditor />)
    const input = screen.getByTestId('sql-input')
    fireEvent.change(input, { target: { value: 'SELECT * FROM t' } })
    fireEvent.click(screen.getByText('Run'))
    await waitFor(() => {
      expect(runSqlQuery).toHaveBeenCalledWith('SELECT * FROM t')
    })
    expect(screen.getByText('id')).toBeInTheDocument()
    expect(screen.getAllByText('1')[0]).toBeInTheDocument()
  })

  it('shows error alert on failure', async () => {
    ;(runSqlQuery as any).mockRejectedValue(new Error('bad query'))
    render(<SQLEditor />)
    const input = screen.getByTestId('sql-input')
    fireEvent.change(input, { target: { value: 'SELECT * FROM bad' } })
    fireEvent.click(screen.getByText('Run'))
    await screen.findByText('bad query')
  })
})
