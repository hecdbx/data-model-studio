// Pure helpers for manipulating the data model and mapping it to React Flow.

let _seq = 0;
export const uid = (p = 'id') => `${p}_${Date.now().toString(36)}_${(_seq++).toString(36)}`;

export const LAYERS = [
  'bronze', 'silver', 'gold', 'fact', 'dimension', 'bridge',
  'hub', 'link', 'satellite', 'table',
];

const LAYER_HEX = {
  bronze: '#8b6139', silver: '#586c7b', gold: '#876813',
  fact: '#226f6a', dimension: '#5065a8', bridge: '#806a35',
  hub: '#2f7285', link: '#277348', satellite: '#7e5e9d', table: '#586e7c',
};
export const layerHex = (l) => LAYER_HEX[l] || LAYER_HEX.table;

const GLYPH = {
  fact: '◆', dimension: '◈', bridge: '⋈', hub: '⬡', link: '◇',
  satellite: '◎', bronze: '▨', silver: '▤', gold: '▥', table: '▦',
};
export const layerGlyph = (l) => GLYPH[l] || GLYPH.table;

export function emptyModel() {
  return {
    model_type: 'physical', name: 'new_model', description: '',
    source: { kind: 'blank' }, entities: [], relationships: [], pipelines: [], lineage: [],
  };
}

export function findEntity(model, ref) {
  return model.entities.find((e) => e.id === ref) ||
         model.entities.find((e) => e.name === ref) || null;
}

// ── Node sizing (keeps dagre + render in sync) ──────────────
const MAX_COLS = 14;
export function nodeSize(entity) {
  const total = entity.columns?.length || 0;
  const shown = Math.min(total, MAX_COLS);
  const height = 56 + (entity.schema ? 20 : 0) + Math.max(shown, 1) * 22 + (total > shown ? 20 : 0);
  return { width: 264, height };
}

// ── model → React Flow ──────────────────────────────────────
export function toFlow(model, { highlight } = {}) {
  const idOf = {};
  model.entities.forEach((e) => { idOf[e.name] = e.id; idOf[e.id] = e.id; });

  const nodes = model.entities.map((e, i) => {
    const size = nodeSize(e);
    return {
      id: e.id,
      type: 'entity',
      ariaLabel: `Table ${e.name}, ${e.columns?.length || 0} columns. Select to edit properties.`,
      position: e.position || { x: (i % 4) * 400, y: Math.floor(i / 4) * 340 },
      width: size.width,
      height: size.height,
      data: { entity: e, dim: highlight ? !highlight.has(e.id) : false },
    };
  });

  const edges = (model.relationships || []).map((r) => {
    const fromCols = (r.from_columns || []).join(', ');
    const toCols = (r.to_columns || []).join(', ');
    // Don't repeat identical key names; full join details remain in Properties.
    const detail = fromCols ? (fromCols === toCols ? fromCols : `${fromCols} → ${toCols || '?'}`) : cardinalityLabel(r.type);
    return {
      id: r.id,
      source: idOf[r.from_entity] || r.from_entity,
      target: idOf[r.to_entity] || r.to_entity,
      type: 'relationship',
      animated: false,
      label: detail,
      ariaLabel: `Relationship from ${r.from_entity} (${fromCols || 'unspecified columns'}) to ${r.to_entity} (${toCols || 'unspecified columns'}), ${r.type || 'many-to-one'} (${cardinalityLabel(r.type || 'many-to-one')}). Select to edit.`,
      labelShowBg: true,
      labelBgPadding: [8, 5],
      labelBgBorderRadius: 5,
      labelStyle: { fill: 'var(--edge-label)', fontFamily: 'var(--mono)', fontWeight: 500, fontSize: 10 },
      labelBgStyle: { fill: 'var(--surface)', stroke: 'var(--edge)', strokeWidth: 1.25 },
      style: { strokeWidth: 1.8, stroke: 'var(--edge)' },
      data: { rel: r, cardinality: cardinalityLabel(r.type), ends: cardinalityEnds(r.type) },
      markerEnd: { type: 'arrowclosed', width: 18, height: 18, color: 'var(--edge)' },
    };
  }).filter((e) => idOf[e.source] && idOf[e.target]);

  // Pipeline lineage (data-flow: bronze → silver → gold), drawn as distinct
  // dashed edges so the cross-layer structure is visible alongside FK links.
  const fkPairs = new Set(edges.map((e) => `${e.source}|${e.target}`));
  const seenLin = new Set();
  const lineageEdges = (model.lineage || []).map((l, i) => {
    const s = idOf[l.source], t = idOf[l.target];
    if (!s || !t || s === t) return null;
    const key = `${s}|${t}`;
    if (fkPairs.has(key) || fkPairs.has(`${t}|${s}`) || seenLin.has(key)) return null;
    seenLin.add(key);
    return {
      id: `lin_${i}_${s}_${t}`,
      source: s,
      target: t,
      type: 'smoothstep',
      animated: false,
      style: { strokeWidth: 1.6, stroke: 'var(--edge-muted)', strokeDasharray: '5 4' },
      markerEnd: { type: 'arrowclosed', width: 15, height: 15, color: 'var(--edge-muted)' },
      data: { lineage: true },
      zIndex: 0,
    };
  }).filter(Boolean);

  return { nodes, edges: [...lineageEdges, ...edges] };
}

function cardinalityLabel(type) {
  switch (type) {
    case 'one-to-many': return '1 : ∞';
    case 'many-to-one': return '∞ : 1';
    case 'one-to-one': return '1 : 1';
    case 'many-to-many': return '∞ : ∞';
    default: return '';
  }
}

// What the diagram prints beside each end of a relationship drawn child → parent.
export function cardinalityEnds(type) {
  switch (type) {
    case 'one-to-one': return { source: '1', target: '1' };
    case 'one-to-many': return { source: '1', target: '∞' };
    case 'many-to-many': return { source: '∞', target: '∞' };
    default: return { source: '∞', target: '1' };
  }
}

const pkOf = (e) => (e?.primary_key?.length ? e.primary_key
  : (e?.columns || []).filter((c) => c.is_primary_key).map((c) => c.name));

// FK columns that are the child's whole primary key allow one child per parent row.
export function inferCardinality(child, fromCols) {
  const pk = new Set(pkOf(child).map((c) => c.toLowerCase()));
  const fk = new Set((fromCols || []).map((c) => c.toLowerCase()));
  return fk.size && fk.size === pk.size && [...fk].every((c) => pk.has(c)) ? 'one-to-one' : 'many-to-one';
}

// ── immutable edits ─────────────────────────────────────────
const clone = (m) => JSON.parse(JSON.stringify(m));

export function withPositions(model, nodes) {
  const pos = {};
  nodes.forEach((n) => { pos[n.id] = { x: Math.round(n.position.x), y: Math.round(n.position.y) }; });
  const m = clone(model);
  m.entities.forEach((e) => { if (pos[e.id]) e.position = pos[e.id]; });
  return m;
}

export function addEntity(model, partial = {}) {
  const m = clone(model);
  const n = m.entities.length + 1;
  const name = partial.name || `new_table_${n}`;
  const id = partial.id || uid('ent');
  m.entities.push({
    id, name, schema: partial.schema || null, catalog: partial.catalog || null,
    layer: partial.layer || 'table', description: '', is_new: true,
    columns: partial.columns || [{ name: 'id', type: 'BIGINT', nullable: false, is_primary_key: true, description: '' }],
    primary_key: partial.primary_key || ['id'],
    position: partial.position || { x: 60, y: 60 },
  });
  return { model: m, id };
}

export function updateEntity(model, id, patch) {
  const m = clone(model);
  const e = m.entities.find((x) => x.id === id);
  if (e) Object.assign(e, patch);
  return m;
}

export function removeEntity(model, id) {
  const m = clone(model);
  m.entities = m.entities.filter((e) => e.id !== id);
  m.relationships = m.relationships.filter((r) => {
    const s = findId(model, r.from_entity), t = findId(model, r.to_entity);
    return s !== id && t !== id;
  });
  // columns elsewhere that referenced the removed table are no longer FKs
  m.entities.forEach((e) => (e.columns || []).forEach((c) => {
    if (c.references && findId(model, c.references.entity) === id) { c.is_foreign_key = false; delete c.references; }
  }));
  return m;
}

function findId(model, ref) {
  const e = findEntity(model, ref);
  return e ? e.id : ref;
}

// Clear the FK flags a relationship put on its child columns, unless another
// relationship from the same child still uses the column.
function clearFkFlags(m, rel) {
  const child = findEntity(m, rel.from_entity);
  if (!child) return;
  const parentId = findId(m, rel.to_entity);
  (rel.from_columns || []).forEach((fc) => {
    const stillUsed = m.relationships.some((r) => r.id !== rel.id
      && findId(m, r.from_entity) === child.id && (r.from_columns || []).includes(fc));
    const col = (child.columns || []).find((c) => c.name === fc);
    if (col && !stillUsed && (!col.references || findId(m, col.references.entity) === parentId)) {
      col.is_foreign_key = false;
      delete col.references;
    }
  });
}

function setFkFlags(m, rel) {
  const child = findEntity(m, rel.from_entity);
  if (!child) return;
  const parentId = findId(m, rel.to_entity);
  (rel.from_columns || []).forEach((fc, i) => {
    const col = (child.columns || []).find((c) => c.name === fc);
    if (col) {
      col.is_foreign_key = true;
      col.references = { entity: parentId, column: (rel.to_columns || [])[i] || (rel.to_columns || [])[0] || '' };
    }
  });
}

export function setColumns(model, entityId, columns) {
  const m = clone(model);
  const e = m.entities.find((x) => x.id === entityId);
  if (e) {
    e.columns = columns;
    e.primary_key = columns.filter((c) => c.is_primary_key).map((c) => c.name);
  }
  return m;
}

export function addColumn(model, entityId) {
  const e = findEntity(model, entityId);
  const cols = [...(e?.columns || []), { name: `col_${(e?.columns?.length || 0) + 1}`, type: 'STRING', nullable: true, is_primary_key: false, description: '' }];
  return setColumns(model, entityId, cols);
}

const singular = (n) => (n.endsWith('ies') && n.length > 4 ? `${n.slice(0, -3)}y`
  : n.endsWith('ses') && n.length > 4 ? n.slice(0, -2)
    : n.endsWith('s') && !n.endsWith('ss') && n.length > 3 ? n.slice(0, -1) : n);
const baseName = (n) => (n || '').toLowerCase().replace(/^(dim|fact|bridge|hub|sat|lnk|link|bronze|silver|gold|stg|staging)_/, '');

export function addRelationship(model, sourceId, targetId) {
  const m = clone(model);
  if (sourceId === targetId) return { model: m, id: null };
  const child = m.entities.find((e) => e.id === sourceId);
  const parent = m.entities.find((e) => e.id === targetId);
  if (!child || !parent) return { model: m, id: null };
  const pk = (parent.primary_key && parent.primary_key.length)
    ? parent.primary_key : parent.columns.filter((c) => c.is_primary_key).map((c) => c.name);
  const childPk = (child.primary_key && child.primary_key.length)
    ? child.primary_key : child.columns.filter((c) => c.is_primary_key).map((c) => c.name);
  let from = [], to = [];
  if (pk.length > 1) {
    // composite key: the child must carry every key column (same names)
    if (pk.every((p) => child.columns.some((c) => c.name === p))) { from = [...pk]; to = [...pk]; }
  } else if (pk.length === 1) {
    const p = pk[0];
    const b = baseName(parent.name), s = singular(b);
    const cand = [p, `${s}_${p}`, `${b}_${p}`, `${s}_id`, `${b}_id`, `${s}_key`, `${s}_sk`];
    // never map onto the child's own generic key (orders.id is not customers.id); a
    // child keyed by the parent's own key (site_power_profiles.site_id) is a 1:1 extension
    const ownKey = (name) => childPk.length === 1 && childPk[0] === name && /^(id|key|uuid|pk|sk)$/i.test(name);
    let fk = cand.map((n) => child.columns.find((c) => c.name === n && !ownKey(n))).find(Boolean);
    if (!fk) {
      // no matching column — add one typed like the parent's key
      const pcol = parent.columns.find((c) => c.name === p);
      const name = p === 'id' ? `${s}_id` : (child.columns.some((c) => c.name === p) ? `${s}_${p}` : p);
      fk = { name, type: pcol?.type || 'BIGINT', nullable: true, is_primary_key: false, description: `FK to ${parent.name}` };
      child.columns.push(fk);
    }
    from = [fk.name]; to = [p];
  }
  const id = uid('rel');
  const rel = { id, from_entity: child.id, from_columns: from, to_entity: parent.id, to_columns: to, type: inferCardinality(child, from), description: '' };
  m.relationships.push(rel);
  setFkFlags(m, rel);
  return { model: m, id };
}

export function removeRelationship(model, relId) {
  const m = clone(model);
  const rel = m.relationships.find((r) => r.id === relId);
  m.relationships = m.relationships.filter((r) => r.id !== relId);
  if (rel) clearFkFlags(m, rel);
  return m;
}

export function updateRelationship(model, relId, patch) {
  const m = clone(model);
  const r = m.relationships.find((x) => x.id === relId);
  if (r) {
    const before = { ...r };
    Object.assign(r, patch);
    // keep the child's column FK flags in step with the edited columns
    if ('from_columns' in patch || 'to_columns' in patch) {
      clearFkFlags(m, before);
      setFkFlags(m, r);
    }
  }
  return m;
}

// Merge an agent result into the canvas model. The agent sends a patch — the
// tables / relationships it added or changed plus explicit removals — so
// anything it didn't mention is kept as-is (a full model merges the same way).
export function mergeModel(model, patch) {
  const m = clone(model);
  const resolve = (ref) => findEntity(m, ref)?.id || ref;
  (patch.entities || []).forEach((pe) => {
    const match = m.entities.find((e) => e.id === pe.id)
      || m.entities.find((e) => e.name === pe.name && (!pe.schema || !e.schema || e.schema === pe.schema));
    if (match) {
      const old = Object.fromEntries((match.columns || []).map((c) => [c.name, c]));
      const columns = (pe.columns || match.columns || []).map((c) => ({ ...(old[c.name] || {}), ...c }));
      Object.assign(match, pe, { id: match.id, position: match.position, columns });
      match.primary_key = (pe.primary_key && pe.primary_key.length)
        ? pe.primary_key : columns.filter((c) => c.is_primary_key).map((c) => c.name);
    } else {
      const id = pe.id || (pe.schema ? `${pe.schema}.${pe.name}` : pe.name) || uid('ent');
      m.entities.push({ is_new: true, ...pe, id, columns: pe.columns || [] });
    }
  });
  const goneE = new Set((patch.removed_entities || []).map(resolve));
  if (goneE.size) {
    m.entities = m.entities.filter((e) => !goneE.has(e.id));
    m.relationships = m.relationships.filter((r) => !goneE.has(resolve(r.from_entity)) && !goneE.has(resolve(r.to_entity)));
  }
  const key = (r) => `${resolve(r.from_entity)}|${(r.from_columns || []).join(',')}|${resolve(r.to_entity)}`;
  const goneR = new Set(patch.removed_relationships || []);
  m.relationships.filter((r) => goneR.has(r.id) || goneR.has(key(r))).forEach((r) => clearFkFlags(m, r));
  m.relationships = m.relationships.filter((r) => !goneR.has(r.id) && !goneR.has(key(r)));
  (patch.relationships || []).forEach((pr) => {
    const r = { ...pr, from_entity: resolve(pr.from_entity), to_entity: resolve(pr.to_entity) };
    if (!findEntity(m, r.from_entity) || !findEntity(m, r.to_entity)) return;
    const i = m.relationships.findIndex((x) => (r.id && x.id === r.id) || key(x) === key(r));
    if (i >= 0) m.relationships[i] = { ...m.relationships[i], ...r, id: m.relationships[i].id };
    else m.relationships.push({ description: '', ...r, type: r.type || inferCardinality(findEntity(m, r.from_entity), r.from_columns), id: r.id || uid('rel') });
    setFkFlags(m, r);
  });
  if (!model.entities.length) {
    ['name', 'description', 'model_type'].forEach((k) => { if (patch[k]) m[k] = patch[k]; });
  }
  return m;
}

// Give tables without a position a spot to the right of the current diagram.
export function placeNewEntities(model) {
  const placed = model.entities.filter((e) => e.position);
  const fresh = model.entities.filter((e) => !e.position);
  if (!fresh.length || !placed.length) return model;
  const m = clone(model);
  const maxX = Math.max(...placed.map((e) => e.position.x + nodeSize(e).width));
  const minY = Math.min(...placed.map((e) => e.position.y));
  let y = minY;
  m.entities.forEach((e) => {
    if (!e.position) { e.position = { x: maxX + 140, y }; y += nodeSize(e).height + 60; }
  });
  return m;
}

export function stats(model) {
  const cols = model.entities.reduce((n, e) => n + (e.columns?.length || 0), 0);
  return { entities: model.entities.length, relationships: model.relationships.length, columns: cols };
}
