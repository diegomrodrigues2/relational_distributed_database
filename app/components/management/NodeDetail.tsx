import React from 'react';
import { Node, NodeStatus } from '../../types';
import Card from '../common/Card';
import Button from '../common/Button';
import StorageInspector from './StorageInspector';

const NodeStatusBadge: React.FC<{ status: NodeStatus }> = ({ status }) => {
  const statusStyles = {
    [NodeStatus.LIVE]: 'bg-green-500/20 text-green-400',
    [NodeStatus.SUSPECT]: 'bg-yellow-500/20 text-yellow-400',
    [NodeStatus.DEAD]: 'bg-red-500/20 text-red-400',
  };
  return (
    <span className={`px-2 py-1 text-xs font-medium rounded-full ${statusStyles[status]}`}>
      {status}
    </span>
  );
};

const StatItem: React.FC<{ label: string; value: React.ReactNode }> = ({ label, value }) => (
    <div className="flex justify-between py-3 border-b border-green-800/50">
        <dt className="text-green-300">{label}</dt>
        <dd className="font-mono text-green-100">{value}</dd>
    </div>
);

interface NodeDetailProps {
  node: Node;
  onRemove: (nodeId: string) => void;
  onStop: (nodeId: string) => void;
  onStart: (nodeId: string) => void;
  isLoading: boolean;
}

const NodeDetail: React.FC<NodeDetailProps> = ({ node, onRemove, onStop, onStart, isLoading }) => {
  return (
    <div className="space-y-6">
        <Card>
            <div className="p-6 border-b border-green-800/60">
                <div className="flex justify-between items-center">
                    <h3 className="text-xl font-semibold text-green-50">Node: {node.id}</h3>
                    <NodeStatusBadge status={node.status} />
                </div>
                <p className="text-green-400 font-mono mt-1">{node.address}</p>
            </div>
            <div className="p-6">
                <h4 className="font-semibold text-green-100 mb-2">Node Vitals</h4>
                <dl>
                    <StatItem label="Uptime" value={node.uptime} />
                    <StatItem label="CPU Usage" value={`${node.cpuUsage.toFixed(1)}%`} />
                    <StatItem label="Memory Usage" value={`${node.memoryUsage.toFixed(1)}%`} />
                    <StatItem label="Disk Usage" value={`${node.diskUsage.toFixed(1)}%`} />
                    <StatItem label="Data Load" value={`${node.dataLoad} GB`} />
                </dl>
            </div>
            <div className="p-4 bg-green-900/10 border-t border-green-800/60 flex justify-end items-center space-x-3">
                    <Button onClick={() => onStart(node.id)} disabled={isLoading || node.status === NodeStatus.LIVE} variant="secondary">
                        Start
                    </Button>
                    <Button onClick={() => onStop(node.id)} disabled={isLoading || node.status === NodeStatus.DEAD} variant="secondary">
                        Stop
                    </Button>
                    <Button onClick={() => onRemove(node.id)} disabled={isLoading} variant="danger">
                        Remove Node
                    </Button>
                </div>
        </Card>
        
        <StorageInspector nodeId={node.id} />

    </div>
  );
};

export default NodeDetail;