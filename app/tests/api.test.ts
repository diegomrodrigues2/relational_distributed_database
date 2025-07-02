import {
  getNodes,
  addNode,
  removeNode,
  startNode,
  stopNode,
  splitPartition,
  mergePartitions,
  rebalance,
} from '../services/api'
import { vi } from 'vitest'

const sample = { nodes: [{ node_id: 'n1', host: 'h', port: 123, status: 'live', cpu: 1, memory: 2, disk: 3, replication_log_size: 0, hints_count: 0 }] }

describe('api service', () => {
  it('maps nodes from API', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: true, json: async () => sample }))
    const res = await getNodes()
    expect(res[0].id).toBe('n1')
    expect(res[0].address).toBe('h:123')
  })

  it('addNode posts to correct endpoint', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce({ ok: true, json: async () => ({ status: 'ok', node_id: 'n2' }) })
    vi.stubGlobal('fetch', fetchMock)
    const id = await addNode()
    expect(id).toBe('n2')
    expect(fetchMock).toHaveBeenCalledWith(
      'http://localhost:8000/cluster/actions/add_node',
      { method: 'POST' },
    )
  })

  it('removeNode sends DELETE to correct endpoint', async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, json: async () => ({ status: 'ok' }) })
    vi.stubGlobal('fetch', fetchMock)
    const nodeId = await removeNode('n1')
    expect(nodeId).toBe('n1')
    expect(fetchMock).toHaveBeenCalledWith(
      'http://localhost:8000/cluster/actions/remove_node/n1',
      { method: 'DELETE' },
    )
  })

  it('startNode posts and returns updated node', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce({ ok: true, json: async () => ({ status: 'ok' }) })
      .mockResolvedValueOnce({ ok: true, json: async () => sample })
    vi.stubGlobal('fetch', fetchMock)
    const node = await startNode('n1')
    expect(node.id).toBe('n1')
    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      'http://localhost:8000/nodes/n1/start',
      { method: 'POST' },
    )
    expect(fetchMock).toHaveBeenNthCalledWith(2, 'http://localhost:8000/cluster/nodes', undefined)
  })

  it('stopNode posts and returns updated node', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce({ ok: true, json: async () => ({ status: 'ok' }) })
      .mockResolvedValueOnce({ ok: true, json: async () => sample })
    vi.stubGlobal('fetch', fetchMock)
    const node = await stopNode('n1')
    expect(node.id).toBe('n1')
    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      'http://localhost:8000/nodes/n1/stop',
      { method: 'POST' },
    )
    expect(fetchMock).toHaveBeenNthCalledWith(2, 'http://localhost:8000/cluster/nodes', undefined)
  })

  it('splitPartition posts pid and split_key', async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, json: async () => ({ status: 'ok' }) })
    vi.stubGlobal('fetch', fetchMock)
    await splitPartition(3, 'k')
    expect(fetchMock).toHaveBeenCalledWith(
      'http://localhost:8000/cluster/actions/split_partition?pid=3&split_key=k',
      { method: 'POST' },
    )
  })

  it('mergePartitions posts pid1 and pid2', async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, json: async () => ({ status: 'ok' }) })
    vi.stubGlobal('fetch', fetchMock)
    await mergePartitions(1, 2)
    expect(fetchMock).toHaveBeenCalledWith(
      'http://localhost:8000/cluster/actions/merge_partitions?pid1=1&pid2=2',
      { method: 'POST' },
    )
  })

  it('rebalance posts to correct endpoint', async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, json: async () => ({ status: 'ok' }) })
    vi.stubGlobal('fetch', fetchMock)
    await rebalance()
    expect(fetchMock).toHaveBeenCalledWith(
      'http://localhost:8000/cluster/actions/rebalance',
      { method: 'POST' },
    )
  })
})
