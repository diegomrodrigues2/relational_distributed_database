import { getTransactions } from '../services/api'
import { vi } from 'vitest'

describe('getTransactions', () => {
  it('parses transactions from API', async () => {
    const sample = { transactions: [ { node: 'n1', tx_ids: ['a', 'b'] } ] }
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: true, json: async () => sample }))
    const res = await getTransactions()
    expect(res).toEqual([
      { node: 'n1', txId: 'a' },
      { node: 'n1', txId: 'b' },
    ])
  })
})
