import React from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { MetricPoint } from '../../types';
import Card from '../common/Card';

interface MetricChartProps {
  title: string;
  data: MetricPoint[];
  lineColor: string;
  unit?: string;
}

const MetricChart: React.FC<MetricChartProps> = ({ title, data, lineColor, unit = '' }) => {
  return (
    <Card className="p-4 h-80">
      <h3 className="text-lg font-semibold text-green-100 mb-4">{title}</h3>
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data} margin={{ top: 5, right: 20, left: -10, bottom: 20 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#2f603b" />
          <XAxis dataKey="time" stroke="#86efac" fontSize={12} />
          <YAxis stroke="#86efac" fontSize={12} unit={unit} />
          <Tooltip
            contentStyle={{
              backgroundColor: '#19271c',
              border: '1px solid #166534',
              borderRadius: '0.5rem',
            }}
            labelStyle={{ color: '#dcfce7' }}
          />
          <Legend />
          <Line type="monotone" dataKey="value" name={title} stroke={lineColor} strokeWidth={2} dot={false} />
        </LineChart>
      </ResponsiveContainer>
    </Card>
  );
};

export default MetricChart;