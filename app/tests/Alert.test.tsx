import { render, screen } from '@testing-library/react'
import '@testing-library/jest-dom'
import { vi } from 'vitest'
import Alert from '../components/common/Alert'

describe('Alert', () => {
  it('calls onClose after timeout', () => {
    vi.useFakeTimers()
    const onClose = vi.fn()
    render(<Alert message="done" type="success" onClose={onClose} />)
    expect(screen.getByText('done')).toBeInTheDocument()
    vi.advanceTimersByTime(3000)
    expect(onClose).toHaveBeenCalled()
  })
})
