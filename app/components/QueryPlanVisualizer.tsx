import React from 'react';

export interface PlanNode {
  node_type: string;
  blocksAccessed?: number;
  recordsOutput?: number;
  children?: PlanNode[];
  [key: string]: any;
}

interface Props {
  plan: PlanNode;
}

const renderNode = (node: PlanNode): JSX.Element => {
  return (
    <li>
      <span title={`blocks: ${node.blocksAccessed ?? 0}, rows: ${node.recordsOutput ?? 0}`}>{node.node_type}</span>
      {node.children && node.children.length > 0 && (
        <ul className="ml-4 list-disc">
          {node.children.map((c, idx) => (
            <React.Fragment key={idx}>{renderNode(c)}</React.Fragment>
          ))}
        </ul>
      )}
    </li>
  );
};

const QueryPlanVisualizer: React.FC<Props> = ({ plan }) => {
  return (
    <div className="p-2">
      <ul className="list-disc">
        {renderNode(plan)}
      </ul>
    </div>
  );
};

export default QueryPlanVisualizer;

