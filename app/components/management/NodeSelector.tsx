import React from 'react';
import { Node, NodeStatus } from '../../types';
import Card from '../common/Card';
import Button from '../common/Button';

const StatusIndicator: React.FC<{ status: NodeStatus }> = ({ status }) => {
    const colorClass = {
        [NodeStatus.LIVE]: 'bg-green-500',
        [NodeStatus.SUSPECT]: 'bg-yellow-500',
        [NodeStatus.DEAD]: 'bg-red-500',
    }[status];
    return <span className={`w-2.5 h-2.5 rounded-full ${colorClass}`} title={status}></span>;
}

interface NodeSelectorProps {
    nodes: Node[];
    selectedNodeId: string | null;
    onSelectNode: (nodeId: string) => void;
    onAddNode: () => void;
    isActionLoading: boolean;
}

const NodeSelector: React.FC<NodeSelectorProps> = ({ nodes, selectedNodeId, onSelectNode, onAddNode, isActionLoading }) => {
  return (
    <Card className="h-full">
        <div className="p-4 border-b border-green-800/60">
            <h3 className="text-lg font-semibold text-green-100">Cluster Nodes</h3>
        </div>
        <div className="p-2 max-h-[calc(100vh-250px)] overflow-y-auto">
            <div className="space-y-1">
            {nodes.map(node => (
                <button
                    key={node.id}
                    onClick={() => onSelectNode(node.id)}
                    className={`w-full text-left p-3 rounded-md transition-colors flex items-center justify-between ${
                        selectedNodeId === node.id 
                        ? 'bg-green-500/20 text-green-300' 
                        : 'hover:bg-green-900/40 text-green-200'
                    }`}
                >
                    <span className="font-medium">{node.id}</span>
                    <StatusIndicator status={node.status} />
                </button>
            ))}
            </div>
        </div>
         <div className="p-3 border-t border-green-800/60">
            <Button onClick={onAddNode} className="w-full" disabled={isActionLoading}>
                {isActionLoading ? 'Adding...' : 'Add New Node'}
            </Button>
        </div>
    </Card>
  );
};

export default NodeSelector;