import React, { useState } from 'react';
import Sidebar from './components/Sidebar';
import Dashboard from './components/Dashboard';
import DataBrowser from './components/DataBrowser';
import Management from './components/Management';
import ClusterInternals from './components/ClusterInternals';

type View = 'dashboard' | 'browser' | 'management' | 'internals';

const App: React.FC = () => {
  const [activeView, setActiveView] = useState<View>('dashboard');
  const [managedNodeId, setManagedNodeId] = useState<string | null>(null);

  const handleManageNode = (nodeId: string) => {
    setManagedNodeId(nodeId);
    setActiveView('management');
  };
  
  const handleSetView = (view: View) => {
    if (view !== 'management') {
      setManagedNodeId(null);
    }
    setActiveView(view);
  };

  const renderView = () => {
    switch (activeView) {
      case 'dashboard':
        return <Dashboard onManageNode={handleManageNode} />;
      case 'browser':
        return <DataBrowser />;
      case 'internals':
        return <ClusterInternals />;
      case 'management':
        return <Management initialSelectedNodeId={managedNodeId} />;
      default:
        return <Dashboard onManageNode={handleManageNode} />;
    }
  };

  return (
    <div className="flex h-screen font-sans bg-[#10180f] text-green-200">
      <Sidebar activeView={activeView} setActiveView={handleSetView} />
      <main className="flex-1 overflow-y-auto p-4 md:p-6 lg:p-8 bg-[#141f17]">
        {renderView()}
      </main>
    </div>
  );
};

export default App;