import React from 'react';
import { Node, NodeStatus } from '../../types';
import Card from '../common/Card';
import Button from '../common/Button';
import { ICONS } from '../../constants';

const NodeStatusBadge: React.FC<{ status: NodeStatus; isCompacting?: boolean; }> = ({ status, isCompacting }) => {
  const statusStyles = {
    [NodeStatus.LIVE]: 'bg-green-500/20 text-green-400',
    [NodeStatus.SUSPECT]: 'bg-yellow-500/20 text-yellow-400',
    [NodeStatus.DEAD]: 'bg-red-500/20 text-red-400',
  };
  return (
    <div className="flex items-center space-x-2">
        <span className={`px-2 py-1 text-xs font-medium rounded-full ${statusStyles[status]}`}>
            {status}
        </span>
        {isCompacting && (
            <span className="text-green-400 animate-spin" title="Compacting">
                {ICONS.cog}
            </span>
        )}
    </div>
  );
};

interface NodeListProps {
  nodes: Node[];
  onManageNode: (nodeId: string) => void;
}

const NodeList: React.FC<NodeListProps> = ({ nodes, onManageNode }) => {
  return (
    <Card className="overflow-x-auto">
      <div className="p-4 border-b border-green-800/60">
        <h3 className="text-lg font-semibold text-green-100">Nodes</h3>
      </div>
      <table className="w-full text-sm text-left text-green-300">
        <thead className="text-xs text-green-400 uppercase bg-green-900/30">
          <tr>
            <th scope="col" className="px-6 py-3">Node ID</th>
            <th scope="col" className="px-6 py-3">Address</th>
            <th scope="col" className="px-6 py-3">Status</th>
            <th scope="col" className="px-6 py-3">Uptime</th>
            <th scope="col" className="px-6 py-3 text-right">CPU</th>
            <th scope="col" className="px-6 py-3 text-right">Memory</th>
            <th scope="col" className="px-6 py-3 text-right">Disk</th>
            <th scope="col" className="px-6 py-3 text-right">Repl. Log</th>
            <th scope="col" className="px-6 py-3 text-right">Hints</th>
            <th scope="col" className="px-6 py-3 text-right">Actions</th>
          </tr>
        </thead>
        <tbody>
          {nodes.map(node => (
            <tr key={node.id} className="border-b border-green-800/60 hover:bg-green-900/20">
              <td className="px-6 py-4 font-medium text-green-100 whitespace-nowrap">{node.id}</td>
              <td className="px-6 py-4">{node.address}</td>
              <td className="px-6 py-4"><NodeStatusBadge status={node.status} isCompacting={node.isCompacting} /></td>
              <td className="px-6 py-4">{node.uptime}</td>
              <td className="px-6 py-4 text-right">{node.cpuUsage.toFixed(1)}%</td>
              <td className="px-6 py-4 text-right">{node.memoryUsage.toFixed(1)}%</td>
              <td className="px-6 py-4 text-right">{node.diskUsage.toFixed(1)}%</td>
              <td className="px-6 py-4 text-right">{node.replicationLogSize}</td>
              <td className="px-6 py-4 text-right">{node.hintsCount}</td>
              <td className="px-6 py-4 text-right">
                <Button variant="secondary" size="sm" onClick={() => onManageNode(node.id)}>
                    Manage
                </Button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </Card>
  );
};

export default NodeList;