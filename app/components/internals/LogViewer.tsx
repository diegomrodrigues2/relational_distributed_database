import React, { useState, useEffect, useCallback } from 'react';
import { getNodes, getClusterEvents, getNodeEvents } from '../../services/api';
import { Node } from '../../types';
import Card from '../common/Card';
import Button from '../common/Button';

const LogViewer: React.FC = () => {
  const [nodes, setNodes] = useState<Node[]>([]);
  const [selected, setSelected] = useState<string>('cluster');
  const [events, setEvents] = useState<string[]>([]);
  const [isLoading, setIsLoading] = useState(true);

  const fetchEvents = useCallback(async () => {
    setIsLoading(true);
    try {
      const data =
        selected === 'cluster'
          ? await getClusterEvents()
          : await getNodeEvents(selected);
      setEvents(data);
    } catch (err) {
      console.error('Failed to fetch events', err);
      setEvents([]);
    } finally {
      setIsLoading(false);
    }
  }, [selected]);

  useEffect(() => {
    getNodes().then(setNodes).catch(err => console.error(err));
  }, []);

  useEffect(() => {
    fetchEvents();
  }, [fetchEvents]);

  return (
    <Card>
      <div className="p-4 border-b border-green-800/60 flex items-center space-x-2">
        <h3 className="text-lg font-semibold text-green-100 flex-1">Event Logs</h3>
        <select
          value={selected}
          onChange={e => setSelected(e.target.value)}
          className="bg-[#10180f] border border-green-700/50 rounded-md px-2 py-1 text-green-200"
        >
          <option value="cluster">Cluster</option>
          {nodes.map(n => (
            <option key={n.id} value={n.id}>{n.id}</option>
          ))}
        </select>
        <Button size="sm" variant="secondary" onClick={fetchEvents}>
          Refresh
        </Button>
      </div>
      <div className="p-4 max-h-96 overflow-y-auto font-mono text-sm bg-[#10180f]">
        {isLoading ? (
          <div className="text-center">Loading...</div>
        ) : events.length > 0 ? (
          <ul className="space-y-1">
            {events.map((e, i) => (
              <li key={i}>{e}</li>
            ))}
          </ul>
        ) : (
          <div className="text-center text-green-400">No events found.</div>
        )}
      </div>
    </Card>
  );
};

export default LogViewer;
