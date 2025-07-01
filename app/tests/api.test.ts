import { getNodes } from '../services/api'
import { vi } from 'vitest'

const sample = { nodes: [{ node_id: 'n1', host: 'h', port: 123, status: 'live', cpu: 1, memory: 2, disk: 3, replication_log_size: 0, hints_count: 0 }] }

describe('api service', () => {
  it('maps nodes from API', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: true, json: async () => sample }))
    const res = await getNodes()
    expect(res[0].id).toBe('n1')
    expect(res[0].address).toBe('h:123')
  })
})
