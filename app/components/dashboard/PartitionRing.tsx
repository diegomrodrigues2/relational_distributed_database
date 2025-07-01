import React, { useMemo } from 'react';
import { Partition } from '../../types';
import Card from '../common/Card';

interface PartitionLoadRingProps {
  partitions: Partition[];
}

const RingSegment: React.FC<{
    startAngle: number;
    endAngle: number;
    radius: number;
    strokeWidth: number;
    color: string;
    partition: Partition;
}> = ({ startAngle, endAngle, radius, strokeWidth, color, partition }) => {
    const startRad = (startAngle - 90) * (Math.PI / 180);
    const endRad = (endAngle - 90) * (Math.PI / 180);

    const x1 = 100 + radius * Math.cos(startRad);
    const y1 = 100 + radius * Math.sin(startRad);
    const x2 = 100 + radius * Math.cos(endRad);
    const y2 = 100 + radius * Math.sin(endRad);

    const largeArcFlag = endAngle - startAngle <= 180 ? 0 : 1;

    const pathData = `M ${x1} ${y1} A ${radius} ${radius} 0 ${largeArcFlag} 1 ${x2} ${y2}`;

    return (
        <g>
            <title>{`Partition: ${partition.id}\nOperations: ${partition.operationCount.toLocaleString()}`}</title>
            <path d={pathData} stroke={color} strokeWidth={strokeWidth} fill="none" className="transition-all duration-300 hover:stroke-white cursor-pointer" />
        </g>
    );
};

const PartitionLoadRing: React.FC<PartitionLoadRingProps> = ({ partitions }) => {
  const totalOperations = useMemo(() => partitions.reduce((acc, p) => acc + p.operationCount, 0), [partitions]);

  if (!totalOperations) {
    return (
        <Card className="p-4 flex items-center justify-center h-full min-h-[320px]">
            <p className="text-green-300">No operations to display in ring.</p>
        </Card>
    );
  }
  
  const maxOps = useMemo(() => Math.max(...partitions.map(p => p.operationCount)), [partitions]);

  const getColorForOps = (ops: number) => {
    const ratio = ops / maxOps;
    if (ratio > 0.75) return '#ef4444'; // red-500
    if (ratio > 0.5) return '#eab308'; // yellow-500
    return '#22c55e'; // green-500
  };

  let currentAngle = 0;
  const segments = partitions.map(partition => {
      const angle = (partition.operationCount / totalOperations) * 360;
      const startAngle = currentAngle;
      const endAngle = currentAngle + angle;
      currentAngle = endAngle;
      return { partition, startAngle, endAngle };
  });

  return (
    <Card className="p-4 h-full min-h-[320px]">
      <h3 className="text-lg font-semibold text-green-100 mb-2">Partition Load Ring</h3>
      <div className="flex items-center justify-center">
        <svg viewBox="0 0 200 200" className="w-full h-full max-w-xs">
          {segments.map(({ partition, startAngle, endAngle }) => (
            <RingSegment
              key={partition.id}
              partition={partition}
              startAngle={startAngle}
              endAngle={endAngle > startAngle ? endAngle - 2 : endAngle}
              radius={80}
              strokeWidth={20}
              color={getColorForOps(partition.operationCount)}
            />
          ))}
           <circle cx="100" cy="100" r="60" fill="#19271c" />
            <text x="100" y="95" textAnchor="middle" fill="#a7f3d0" fontSize="10">Total Ops</text>
            <text x="100" y="115" textAnchor="middle" fill="#d1fae5" fontSize="16" fontWeight="bold">
                {`${totalOperations.toLocaleString()}`}
            </text>
        </svg>
      </div>
       <div className="flex justify-center space-x-4 mt-4 text-xs text-green-300">
          <div className="flex items-center"><div className="w-3 h-3 rounded-full bg-green-500 mr-2"></div>Normal</div>
          <div className="flex items-center"><div className="w-3 h-3 rounded-full bg-yellow-500 mr-2"></div>Elevated</div>
          <div className="flex items-center"><div className="w-3 h-3 rounded-full bg-red-500 mr-2"></div>Hot</div>
        </div>
    </Card>
  );
};

export default PartitionLoadRing;