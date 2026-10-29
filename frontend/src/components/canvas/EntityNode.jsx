import React, { memo } from 'react';
import { Handle, Position } from '@xyflow/react';
import { Icon } from '../../lib/icons';

const MAX = 14;

function EntityNode({ data, selected }) {
  const e = data.entity;
  const cols = e.columns || [];
  const shown = cols.slice(0, MAX);
  const extra = cols.length - shown.length;
  const layer = `var(--layer-${e.layer || 'table'}, var(--layer-table))`;

  return (
    <div className={`enode ${selected ? 'selected' : ''} ${data.dim ? 'dim' : ''}`} style={{ '--layer': layer }}>
      <Handle id="l" type="source" position={Position.Left} />
      <Handle id="r" type="source" position={Position.Right} />

      <div className="enode-head">
        <span className="eglyph"><Icon name="table" className="icon-sm" /></span>
        <span className="etitle" title={e.name}>{e.name}</span>
        {e.layer && <span className="elayer">{e.layer}</span>}
      </div>
      {e.schema && <div className="enode-schema"><Icon name="database" />{e.schema}</div>}

      <div className="enode-cols">
        {shown.map((c, i) => (
          <div key={i} className={`ecol ${c.is_primary_key ? 'is-pk' : ''}`}>
            <span>
              {c.is_primary_key ? <span className="kbadge pk" title="Primary key">PK</span>
                : c.is_foreign_key ? <span className="kbadge fk" title="Foreign key">FK</span> : <span className="column-dot" />}
            </span>
            <span className="cname" title={c.name}>{c.name}</span>
            <span className="ctype">{String(c.type || '').toLowerCase()}</span>
          </div>
        ))}
        {extra > 0 && <div className="ecol-more">+{extra} more column{extra > 1 ? 's' : ''}</div>}
        {cols.length === 0 && <div className="ecol-more">No columns yet</div>}
      </div>
    </div>
  );
}

export default memo(EntityNode);
