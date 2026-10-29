import React, { memo } from 'react';
import { BaseEdge, getSmoothStepPath, Position } from '@xyflow/react';

// Cardinality sits just outside each table, above the line: "1" or "∞".
function endLabel(x, y, position) {
  const right = position === Position.Right;
  return { x: x + (right ? 10 : -10), y: y - 7, anchor: right ? 'start' : 'end' };
}

function RelationshipEdge({
  id, sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition, data, markerEnd, style,
  label, labelStyle, labelShowBg, labelBgStyle, labelBgPadding, labelBgBorderRadius, interactionWidth,
}) {
  const [path, labelX, labelY] = getSmoothStepPath({ sourceX, sourceY, sourcePosition, targetX, targetY, targetPosition });
  const ends = data?.ends;
  const s = endLabel(sourceX, sourceY, sourcePosition);
  const t = endLabel(targetX, targetY, targetPosition);
  return (
    <>
      <BaseEdge id={id} path={path} markerEnd={markerEnd} style={style} interactionWidth={interactionWidth}
        label={label} labelX={labelX} labelY={labelY} labelStyle={labelStyle} labelShowBg={labelShowBg}
        labelBgStyle={labelBgStyle} labelBgPadding={labelBgPadding} labelBgBorderRadius={labelBgBorderRadius} />
      {ends && (
        <g className="rel-ends" aria-hidden="true">
          <text className="rel-end" x={s.x} y={s.y} textAnchor={s.anchor}>{ends.source}</text>
          <text className="rel-end" x={t.x} y={t.y} textAnchor={t.anchor}>{ends.target}</text>
        </g>
      )}
    </>
  );
}

export default memo(RelationshipEdge);
