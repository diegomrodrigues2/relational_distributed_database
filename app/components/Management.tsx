import React, { useState, useEffect, useCallback } from 'react';
import { getNodes } from '../services/api';
import { addNode, removeNode, stopNode, startNode } from '../services/api';
import { Node } from '../types';
import NodeSelector from './management/NodeSelector';
import NodeDetail from './management/NodeDetail';
import ClusterActions from './management/ClusterActions';
import PartitionActions from './management/PartitionActions';
import Alert from './common/Alert';

interface ManagementProps {
  initialSelectedNodeId: string | null;
}

const Management: React.FC<ManagementProps> = ({ initialSelectedNodeId }) => {
  const [nodes, setNodes] = useState<Node[]>([]);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(initialSelectedNodeId);
  const [isLoading, setIsLoading] = useState(true);
  const [isActionLoading, setIsActionLoading] = useState(false);
  const [alert, setAlert] = useState<{ message: string; type: 'success' | 'error' } | null>(null);

  const fetchNodes = useCallback(async () => {
    setIsLoading(true);
    try {
      const nodesData = await getNodes();
      setNodes(nodesData);
    } catch (error) {
      console.error("Failed to fetch nodes:", error);
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchNodes();
  }, [fetchNodes]);
  
  useEffect(() => {
    if(initialSelectedNodeId) {
        setSelectedNodeId(initialSelectedNodeId);
    }
  }, [initialSelectedNodeId]);

  const handleSelectNode = (nodeId: string) => {
    setSelectedNodeId(nodeId);
  };

  const handleUpdateNode = (updatedNode: Node) => {
    setNodes(prevNodes => prevNodes.map(n => n.id === updatedNode.id ? updatedNode : n));
  };
  
  const handleAddNode = async () => {
    setIsActionLoading(true);
    try {
        await addNode();
        await fetchNodes(); // Re-fetch all nodes to get the new list
        setAlert({ message: 'Node added', type: 'success' });
    } catch (error) {
        console.error("Failed to add node:", error);
        setAlert({ message: 'Failed to add node', type: 'error' });
    } finally {
        setIsActionLoading(false);
    }
  };

  const handleRemoveNode = async (nodeId: string) => {
    if (window.confirm(`Are you sure you want to remove ${nodeId}? This action is permanent.`)) {
        setIsActionLoading(true);
        try {
            await removeNode(nodeId);
            setSelectedNodeId(null);
            await fetchNodes(); // Re-fetch all nodes
            setAlert({ message: 'Node removed', type: 'success' });
        } catch (error) {
            console.error("Failed to remove node:", error);
            setAlert({ message: 'Failed to remove node', type: 'error' });
        } finally {
            setIsActionLoading(false);
        }
    }
  };
  
  const handleStopNode = async (nodeId: string) => {
    setIsActionLoading(true);
    try {
        const updatedNode = await stopNode(nodeId);
        handleUpdateNode(updatedNode);
        setAlert({ message: 'Node stopped', type: 'success' });
    } catch (error) {
        console.error("Failed to stop node:", error);
        setAlert({ message: 'Failed to stop node', type: 'error' });
    } finally {
        setIsActionLoading(false);
    }
  };

  const handleStartNode = async (nodeId: string) => {
    setIsActionLoading(true);
    try {
        const updatedNode = await startNode(nodeId);
        handleUpdateNode(updatedNode);
        setAlert({ message: 'Node started', type: 'success' });
    } catch (error) {
        console.error("Failed to start node:", error);
        setAlert({ message: 'Failed to start node', type: 'error' });
    } finally {
        setIsActionLoading(false);
    }
  };

  const selectedNode = nodes.find(n => n.id === selectedNodeId) || null;

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="animate-spin rounded-full h-16 w-16 border-t-2 border-b-2 border-green-500"></div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {alert && (
        <Alert
          message={alert.message}
          type={alert.type}
          onClose={() => setAlert(null)}
        />
      )}
       <div>
        <h1 className="text-3xl font-bold text-white">Cluster Management</h1>
        <p className="text-green-300 mt-1">Perform administrative actions on the cluster and its nodes.</p>
      </div>
      <div className="flex flex-col md:flex-row gap-6 lg:gap-8">
        <div className="w-full md:w-1/3 lg:w-1/4">
          <NodeSelector
            nodes={nodes}
            selectedNodeId={selectedNodeId}
            onSelectNode={handleSelectNode}
            onAddNode={handleAddNode}
            isActionLoading={isActionLoading}
          />
        </div>
        <div className="flex-1 space-y-6">
          {selectedNode ? (
            <NodeDetail
                node={selectedNode}
                onRemove={handleRemoveNode}
                onStop={handleStopNode}
                onStart={handleStartNode}
                isLoading={isActionLoading}
            />
          ) : (
            <ClusterActions onAddNode={handleAddNode} isLoading={isActionLoading}/>
          )}
          <PartitionActions />
        </div>
      </div>
    </div>
  );
};

export default Management;