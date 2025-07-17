import React, { useState, useEffect } from 'react';
import CodeMirror from '@uiw/react-codemirror';
import { sql as sqlLang } from '@codemirror/lang-sql';
import Button from './common/Button';
import Card from './common/Card';
import { runSqlQuery, executeSql } from '../services/api';
import Alert from './common/Alert';

interface ColumnDef {
  name: string;
  type: string;
}

const HISTORY_KEY = 'sql_history';

const SQLEditor: React.FC = () => {
  const [sql, setSql] = useState('');
  const [columns, setColumns] = useState<ColumnDef[]>([]);
  const [rows, setRows] = useState<any[]>([]);
  const [history, setHistory] = useState<string[]>([]);
  const [alert, setAlert] = useState<{ message: string; type: 'success' | 'error' } | null>(null);

  useEffect(() => {
    const h = localStorage.getItem(HISTORY_KEY);
    if (h) setHistory(JSON.parse(h));
  }, []);

  const saveHistory = (q: string) => {
    const h = [q, ...history.filter(x => x !== q)].slice(0, 20);
    setHistory(h);
    localStorage.setItem(HISTORY_KEY, JSON.stringify(h));
  };

  const execute = async () => {
    const trimmed = sql.trim();
    if (!trimmed) return;
    saveHistory(trimmed);
    try {
      if (trimmed.toUpperCase().startsWith('SELECT')) {
        const res = await runSqlQuery(trimmed);
        setColumns(res.columns);
        setRows(res.rows);
      } else {
        await executeSql(trimmed);
        setColumns([]);
        setRows([]);
        setAlert({ message: 'Statement executed', type: 'success' });
      }
    } catch (err: any) {
      console.error('SQL execution failed:', err);
      setAlert({ message: err.message || 'Failed to execute SQL', type: 'error' });
    }
  };

  return (
    <div className="space-y-4">
      <Card className="p-4 space-y-2">
        <CodeMirror
          value={sql}
          height="200px"
          extensions={[sqlLang()]}
          onChange={(v) => setSql(v)}
        />
        <textarea data-testid="sql-input" value={sql} onChange={e => setSql(e.target.value)} className="hidden" />
        <div className="flex space-x-2">
          <Button onClick={execute}>Run</Button>
        </div>
      </Card>
      {alert && (
        <Alert
          message={alert.message}
          type={alert.type}
          onClose={() => setAlert(null)}
        />
      )}
      {columns.length > 0 && (
        <Card className="p-4 overflow-auto">
          <table className="min-w-full text-sm text-left text-green-300">
            <thead className="text-xs text-green-400 uppercase bg-green-900/30">
              <tr>
                {columns.map(col => (
                  <th key={col.name} className="px-4 py-2">{col.name}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, idx) => (
                <tr key={idx} className="border-b border-green-800/60">
                  {columns.map(col => (
                    <td key={col.name} className="px-4 py-2 font-mono">{String(row[col.name] ?? '')}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </Card>
      )}
    </div>
  );
};

export default SQLEditor;
