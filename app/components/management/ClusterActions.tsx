import React from 'react';
import Card from '../common/Card';
import Button from '../common/Button';
import { rebalance, resetMetrics } from '../../services/api';

interface ClusterActionsProps {
    onAddNode: () => void;
    isLoading: boolean;
}

const ClusterActions: React.FC<ClusterActionsProps> = ({ onAddNode, isLoading }) => {
  const handleRebalance = async () => {
    try {
      await rebalance();
    } catch (err) {
      console.error('Failed to rebalance cluster:', err);
    }
  };

  const handleResetMetrics = async () => {
    try {
      await resetMetrics();
    } catch (err) {
      console.error('Failed to reset metrics:', err);
    }
  };
  return (
    <Card className="p-6">
      <h3 className="text-xl font-semibold text-green-50 mb-4">Cluster Actions</h3>
      <p className="text-green-300 mb-6">
        Perform actions that affect the entire cluster. Select a node from the list to see node-specific actions.
      </p>
      <div className="space-y-4">
        <div className="flex items-center justify-between p-4 bg-green-900/20 rounded-lg">
            <div>
                <h4 className="font-semibold text-green-100">Add New Node</h4>
                <p className="text-sm text-green-400">Introduce a new, empty node to the cluster.</p>
            </div>
            <Button onClick={onAddNode} disabled={isLoading}>
                {isLoading ? 'Adding...' : 'Add Node'}
            </Button>
        </div>
        <div className="flex items-center justify-between p-4 bg-green-900/20 rounded-lg">
            <div>
                <h4 className="font-semibold text-green-100">Trigger Cluster Rebalance</h4>
                <p className="text-sm text-green-400">Manually start a process to redistribute data evenly.</p>
            </div>
            <Button onClick={handleRebalance} variant="secondary">
                Rebalance
            </Button>
        </div>
        <div className="flex items-center justify-between p-4 bg-green-900/20 rounded-lg">
            <div>
                <h4 className="font-semibold text-green-100">Reset Metrics</h4>
                <p className="text-sm text-green-400">Clear hotspot counters for partitions and keys.</p>
            </div>
            <Button onClick={handleResetMetrics} variant="secondary">
                Reset Metrics
            </Button>
        </div>
      </div>
    </Card>
  );
};

export default ClusterActions;