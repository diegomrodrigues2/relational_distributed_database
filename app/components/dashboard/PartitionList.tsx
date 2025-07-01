import React from 'react';
import { Partition } from '../../types';
import Card from '../common/Card';

interface PartitionListProps {
  partitions: Partition[];
}

const PartitionList: React.FC<PartitionListProps> = ({ partitions }) => {
  return (
    <Card className="overflow-x-auto">
      <div className="p-4 border-b border-green-800/60">
        <h3 className="text-lg font-semibold text-green-100">Partitions</h3>
      </div>
      <table className="w-full text-sm text-left text-green-300">
        <thead className="text-xs text-green-400 uppercase bg-green-900/30">
          <tr>
            <th scope="col" className="px-6 py-3">Partition ID</th>
            <th scope="col" className="px-6 py-3">Key Range</th>
            <th scope="col" className="px-6 py-3">Primary</th>
            <th scope="col" className="px-6 py-3">Replicas</th>
            <th scope="col" className="px-6 py-3 text-right">Size (GB)</th>
            <th scope="col" className="px-6 py-3 text-right">Item Count</th>
          </tr>
        </thead>
        <tbody>
          {partitions.map(partition => (
            <tr key={partition.id} className="border-b border-green-800/60 hover:bg-green-900/20">
              <td className="px-6 py-4 font-medium text-green-100 whitespace-nowrap">{partition.id}</td>
              <td className="px-6 py-4 font-mono text-green-400">{`[${partition.keyRange[0]}, ${partition.keyRange[1]})`}</td>
              <td className="px-6 py-4">{partition.primaryNodeId}</td>
              <td className="px-6 py-4">{partition.replicaNodeIds.join(', ')}</td>
              <td className="px-6 py-4 text-right">{partition.size}</td>
              <td className="px-6 py-4 text-right">{partition.itemCount.toLocaleString()}</td>
            </tr>
          ))}
        </tbody>
      </table>
       {partitions.length === 0 && (
        <div className="text-center py-8 text-green-400">
          No partitions found.
        </div>
      )}
    </Card>
  );
};

export default PartitionList;