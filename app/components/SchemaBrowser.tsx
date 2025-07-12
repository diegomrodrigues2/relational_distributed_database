import React, { useEffect, useState } from 'react';
import {
  getTableList,
  getTableSchema,
  getTableStats,
  getColumnStats,
  analyzeTable,
} from '../services/api';
import { TableSchema, TableStats, ColumnStats } from '../types';

const SchemaBrowser: React.FC = () => {
  const [tables, setTables] = useState<string[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [schema, setSchema] = useState<TableSchema | null>(null);
  const [loading, setLoading] = useState(false);
  const [activeTab, setActiveTab] = useState<'schema' | 'stats'>('schema');
  const [tableStats, setTableStats] = useState<TableStats | null>(null);
  const [columnStats, setColumnStats] = useState<ColumnStats[]>([]);

  useEffect(() => {
    const load = async () => {
      try {
        const list = await getTableList();
        setTables(list);
      } catch (err) {
        console.error('Failed to fetch tables', err);
      }
    };
    load();
  }, []);

  const handleSelect = async (name: string) => {
    setSelected(name);
    setLoading(true);
    try {
      const sch = await getTableSchema(name);
      setSchema(sch);
      setActiveTab('schema');
      setTableStats(null);
      setColumnStats([]);
    } catch (err) {
      console.error('Failed to fetch schema', err);
      setSchema(null);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    const loadStats = async () => {
      if (!selected || activeTab !== 'stats') return;
      try {
        const tbl = await getTableStats(selected);
        const cols = await getColumnStats(selected);
        setTableStats(tbl);
        setColumnStats(cols);
      } catch (err) {
        console.error('Failed to fetch stats', err);
        setTableStats(null);
        setColumnStats([]);
      }
    };
    loadStats();
  }, [selected, activeTab]);

  const handleAnalyze = async () => {
    if (!selected) return;
    await analyzeTable(selected);
    const tbl = await getTableStats(selected);
    const cols = await getColumnStats(selected);
    setTableStats(tbl);
    setColumnStats(cols);
  };

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold text-white">Schema Browser</h1>
        <p className="text-green-300 mt-1">Explore tables and columns.</p>
      </div>
      <div className="flex h-96 border border-green-800/60 rounded-md overflow-hidden">
        <div className="w-1/3 border-r border-green-800/60 overflow-y-auto">
          <ul>
            {tables.map(t => (
              <li
                key={t}
                onClick={() => handleSelect(t)}
                className={`px-4 py-2 cursor-pointer hover:bg-green-900/40 ${selected === t ? 'bg-green-900/60' : ''}`}
              >
                {t}
              </li>
            ))}
          </ul>
        </div>
        <div className="flex-1 p-4 overflow-y-auto">
          {loading ? (
            <div>Loading...</div>
          ) : schema ? (
            <>
              <h2 className="text-xl font-semibold text-green-100 mb-2">{schema.name}</h2>
              <div className="flex space-x-4 mb-4">
                <button
                  onClick={() => setActiveTab('schema')}
                  className={`px-3 py-1 rounded-md text-sm ${activeTab === 'schema' ? 'bg-green-600 text-white' : 'text-green-200 bg-green-900/40'}`}
                >
                  Schema
                </button>
                <button
                  onClick={() => setActiveTab('stats')}
                  className={`px-3 py-1 rounded-md text-sm ${activeTab === 'stats' ? 'bg-green-600 text-white' : 'text-green-200 bg-green-900/40'}`}
                >
                  Estatísticas
                </button>
              </div>
              {activeTab === 'schema' ? (
                <>
                  <div>
                    <h3 className="font-medium text-green-200">Columns</h3>
                    <ul className="list-disc pl-5">
                      {schema.columns.map(c => (
                        <li key={c.name}>
                          <span className="text-green-100">{c.name}</span>{' '}
                          <span className="text-green-400">{c.data_type}</span>
                          {c.primary_key && <span className="ml-2 text-green-500">PK</span>}
                        </li>
                      ))}
                    </ul>
                  </div>
                  {schema.indexes && schema.indexes.length > 0 && (
                    <div className="mt-4">
                      <h3 className="font-medium text-green-200">Indexes</h3>
                      <ul className="list-disc pl-5">
                        {schema.indexes.map(i => (
                          <li key={i.name}>
                            <span className="text-green-100">{i.name}</span>{' '}
                            (<span>{i.columns.join(', ')}</span>)
                          </li>
                        ))}
                      </ul>
                    </div>
                  )}
                </>
              ) : (
                <>
                  <div className="flex space-x-4 mb-4">
                    <button
                      onClick={handleAnalyze}
                      className="px-3 py-1 bg-green-700 text-white rounded-md"
                    >
                      Analisar Tabela
                    </button>
                  </div>
                  <div className="grid grid-cols-2 gap-4 mb-4">
                    <div className="bg-green-900/40 p-2 rounded">Registros: {tableStats?.num_rows ?? 0}</div>
                    <div className="bg-green-900/40 p-2 rounded">Blocos: 0</div>
                  </div>
                  <table className="w-full text-sm text-left text-green-300">
                    <thead className="text-xs text-green-400 uppercase bg-green-900/30">
                      <tr>
                        <th className="px-4 py-2">Coluna</th>
                        <th className="px-4 py-2"># Distintos</th>
                      </tr>
                    </thead>
                    <tbody>
                      {columnStats.map(c => (
                        <tr key={c.col_name} className="border-b border-green-800/50">
                          <td className="px-4 py-2">{c.col_name}</td>
                          <td className="px-4 py-2">{c.num_distinct}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </>
              )}
            </>
          ) : (
            <div>Select a table to view details</div>
          )}
        </div>
      </div>
    </div>
  );
};

export default SchemaBrowser;
