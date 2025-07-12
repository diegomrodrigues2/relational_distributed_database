import React, { useEffect, useState } from 'react';
import { getTableList, getTableSchema } from '../services/api';
import { TableSchema } from '../types';

const SchemaBrowser: React.FC = () => {
  const [tables, setTables] = useState<string[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [schema, setSchema] = useState<TableSchema | null>(null);
  const [loading, setLoading] = useState(false);

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
    } catch (err) {
      console.error('Failed to fetch schema', err);
      setSchema(null);
    } finally {
      setLoading(false);
    }
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
            <div>Select a table to view details</div>
          )}
        </div>
      </div>
    </div>
  );
};

export default SchemaBrowser;
