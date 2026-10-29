// Auto-layout via dagre (bundled — no CDN).
//
// Real ER graphs are often sparse (analytics/gold schemas may have few or no FK
// constraints). Laying the whole graph with one dagre pass then stacks the
// disconnected tables in a single column. Instead we lay out each connected
// component with dagre and shelf-pack the components into a grid.
import dagre from '@dagrejs/dagre';

const W = (n) => n.width || 264;
const H = (n) => n.height || 140;

function connectedComponents(nodes, edges) {
  const adj = new Map(nodes.map((n) => [n.id, []]));
  edges.forEach((e) => {
    if (adj.has(e.source) && adj.has(e.target)) {
      adj.get(e.source).push(e.target);
      adj.get(e.target).push(e.source);
    }
  });
  const seen = new Set();
  const comps = [];
  for (const n of nodes) {
    if (seen.has(n.id)) continue;
    const stack = [n.id];
    const group = [];
    seen.add(n.id);
    while (stack.length) {
      const id = stack.pop();
      group.push(id);
      for (const m of adj.get(id) || []) {
        if (!seen.has(m)) { seen.add(m); stack.push(m); }
      }
    }
    comps.push(group);
  }
  return comps;
}

function layoutComponent(nodeById, ids, edges, direction) {
  const g = new dagre.graphlib.Graph();
  g.setDefaultEdgeLabel(() => ({}));
  g.setGraph({ rankdir: direction, nodesep: 68, ranksep: 168, marginx: 0, marginy: 0 });
  ids.forEach((id) => g.setNode(id, { width: W(nodeById[id]), height: H(nodeById[id]) }));
  const idset = new Set(ids);
  edges.forEach((e) => { if (idset.has(e.source) && idset.has(e.target)) g.setEdge(e.source, e.target); });
  dagre.layout(g);

  const pos = {};
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  ids.forEach((id) => {
    const p = g.node(id);
    const x = p.x - W(nodeById[id]) / 2;
    const y = p.y - H(nodeById[id]) / 2;
    pos[id] = { x, y };
    minX = Math.min(minX, x); minY = Math.min(minY, y);
    maxX = Math.max(maxX, x + W(nodeById[id])); maxY = Math.max(maxY, y + H(nodeById[id]));
  });
  ids.forEach((id) => { pos[id].x -= minX; pos[id].y -= minY; });
  return { ids, pos, w: maxX - minX, h: maxY - minY };
}

export function autoLayout(nodes, edges, direction = 'LR') {
  if (!nodes.length) return nodes;
  const nodeById = Object.fromEntries(nodes.map((n) => [n.id, n]));
  const comps = connectedComponents(nodes, edges)
    .map((ids) => layoutComponent(nodeById, ids, edges, direction))
    .sort((a, b) => (b.ids.length - a.ids.length) || (b.h - a.h));

  const GAP = 120;
  const maxRowW = Math.max(1800, Math.ceil(Math.sqrt(nodes.length)) * 520);
  let x = 0, y = 0, rowH = 0;
  for (const c of comps) {
    if (x > 0 && x + c.w > maxRowW) { x = 0; y += rowH + GAP; rowH = 0; }
    c._ox = x; c._oy = y;
    x += c.w + GAP;
    rowH = Math.max(rowH, c.h);
  }

  const out = nodes.map((n) => ({ ...n }));
  const byId = Object.fromEntries(out.map((n) => [n.id, n]));
  for (const c of comps) {
    for (const id of c.ids) {
      byId[id].position = { x: c._ox + c.pos[id].x, y: c._oy + c.pos[id].y };
    }
  }
  return out;
}
