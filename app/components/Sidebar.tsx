import React from 'react';
import { ICONS } from '../constants';

type View = 'dashboard' | 'browser' | 'schema' | 'transactions' | 'management' | 'internals' | 'logs' | 'sql';

interface SidebarProps {
  activeView: View;
  setActiveView: (view: View) => void;
}

const NavItem: React.FC<{
  icon: React.ReactNode;
  label: string;
  isActive: boolean;
  onClick: () => void;
}> = ({ icon, label, isActive, onClick }) => {
  return (
    <button
      onClick={onClick}
      className={`flex items-center w-full px-4 py-3 text-sm font-medium rounded-lg transition-colors duration-200 ${
        isActive
          ? 'bg-green-600 text-white'
          : 'text-green-300 hover:bg-green-900/30 hover:text-white'
      }`}
    >
      {icon}
      <span className="ml-3">{label}</span>
    </button>
  );
};

const Sidebar: React.FC<SidebarProps> = ({ activeView, setActiveView }) => {
  return (
    <aside className="w-64 flex-shrink-0 bg-[#10180f] border-r border-green-900/50 flex flex-col">
      <div className="h-16 flex items-center px-6 border-b border-green-900/50">
        <span className="mr-3">{ICONS.logo}</span>
        <h1 className="text-xl font-bold text-white">DB UI</h1>
      </div>
      <nav className="flex-1 px-4 py-6 space-y-2">
        <NavItem
          icon={ICONS.dashboard}
          label="Dashboard"
          isActive={activeView === 'dashboard'}
          onClick={() => setActiveView('dashboard')}
        />
        <NavItem
          icon={ICONS.browser}
          label="Data Browser"
          isActive={activeView === 'browser'}
          onClick={() => setActiveView('browser')}
        />
        <NavItem
          icon={ICONS.cog}
          label="Schema Browser"
          isActive={activeView === 'schema'}
          onClick={() => setActiveView('schema')}
        />
        <NavItem
          icon={ICONS.transaction}
          label="Transactions"
          isActive={activeView === 'transactions'}
          onClick={() => setActiveView('transactions')}
        />
        <NavItem
          icon={ICONS.internals}
          label="Cluster Internals"
          isActive={activeView === 'internals'}
          onClick={() => setActiveView('internals')}
        />
        <NavItem
          icon={ICONS.code}
          label="SQL Editor"
          isActive={activeView === 'sql'}
          onClick={() => setActiveView('sql')}
        />
        <NavItem
          icon={ICONS.browser}
          label="Logs"
          isActive={activeView === 'logs'}
          onClick={() => setActiveView('logs')}
        />
        <div className="border-t border-green-900/50 my-4"></div>
         <NavItem
          icon={ICONS.management}
          label="Node Management"
          isActive={activeView === 'management'}
          onClick={() => setActiveView('management')}
        />
      </nav>
      <div className="px-4 py-4 border-t border-green-900/50">
        <div className="flex items-center">
            {ICONS.user}
          <div className="ml-3">
            <p className="text-sm font-medium text-green-100">Admin User</p>
            <p className="text-xs text-green-400">admin@db.local</p>
          </div>
        </div>
      </div>
    </aside>
  );
};

export default Sidebar;