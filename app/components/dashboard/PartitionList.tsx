import React, { useState, useMemo } from 'react';
import { Partition, Node } from '../../types';
import Card from '../common/Card';
import Pagination from '../databrowser/Pagination';

interface PartitionListProps {
  partitions: Partition[];
  nodes: Node[];
  strategy?: string;
}
const PartitionList: React.FC<PartitionListProps> = ({ partitions, nodes, strategy }) => {
  const [searchTerm, setSearchTerm] = useState('');
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);

  const filtered = useMemo(
    () =>
      partitions.filter(p =>
        p.id.toString().toLowerCase().includes(searchTerm.toLowerCase()),
      ),
    [partitions, searchTerm],
  );

  const paginated = useMemo(
    () => filtered.slice((currentPage - 1) * pageSize, currentPage * pageSize),
    [filtered, currentPage, pageSize],
  );

  const hasNext = currentPage * pageSize < filtered.length;
  const nodesById = useMemo(() => {
    const map = new Map<string, Node>();
    nodes.forEach(n => map.set(n.id, n));
    return map;
  }, [nodes]);

  return (
    <Card className="overflow-x-auto">
      <div className="p-4 border-b border-green-800/60 flex justify-between items-center">
        <h3 className="text-lg font-semibold text-green-100">Partitions</h3>
        <input
          type="text"
          placeholder="Filter by ID..."
          value={searchTerm}
          onChange={e => {
            setSearchTerm(e.target.value);
            setCurrentPage(1);
          }}
          className="bg-[#10180f] border border-green-700/50 rounded-md px-2 py-1 text-green-200"
        />
      </div>
      <table className="w-full text-sm text-left text-green-300">
        <thead className="text-xs text-green-400 uppercase bg-green-900/30">
          <tr>
            <th scope="col" className="px-6 py-3">Partition ID</th>
            <th scope="col" className="px-6 py-3">Key Range</th>
            <th scope="col" className="px-6 py-3">Owner Node</th>
            <th scope="col" className="px-6 py-3">Replicas</th>
            <th scope="col" className="px-6 py-3">Type</th>
            <th scope="col" className="px-6 py-3 text-right">Size (GB)</th>
            <th scope="col" className="px-6 py-3 text-right">Item Count</th>
          </tr>
        </thead>
        <tbody>
          {paginated.map(partition => {
            const owner = nodesById.get(partition.primaryNodeId);
            return (
              <tr key={partition.id} className="border-b border-green-800/60 hover:bg-green-900/20">
                <td className="px-6 py-4 font-medium text-green-100 whitespace-nowrap">{partition.id}</td>
                <td className="px-6 py-4 font-mono text-green-400">{`[${partition.keyRange[0]}, ${partition.keyRange[1]})`}</td>
                <td className="px-6 py-4">
                  {owner ? `${owner.id} (${owner.address})` : partition.primaryNodeId}
                </td>
                <td className="px-6 py-4">{partition.replicaNodeIds.join(', ')}</td>
                <td className="px-6 py-4">{strategy ? strategy : '-'}</td>
                <td className="px-6 py-4 text-right">{partition.size}</td>
                <td className="px-6 py-4 text-right">{partition.itemCount.toLocaleString()}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
       {filtered.length === 0 && (
        <div className="text-center py-8 text-green-400">
          No partitions found.
        </div>
      )}
      <Pagination
        currentPage={currentPage}
        onPageChange={setCurrentPage}
        pageSize={pageSize}
        onPageSizeChange={(size) => { setPageSize(size); setCurrentPage(1); }}
        disablePrev={currentPage === 1}
        disableNext={!hasNext}
      />
    </Card>
  );
};

export default PartitionList;