import React, { useCallback, useEffect, useMemo, useRef } from 'react';
import {
  ReactFlow, ReactFlowProvider, Background, Controls, MiniMap,
  useNodesState, useEdgesState, useReactFlow, ConnectionMode, BackgroundVariant,
} from '@xyflow/react';
import EntityNode from './EntityNode';
import RelationshipEdge from './RelationshipEdge';
import { Icon } from '../../lib/icons';
import { autoLayout } from '../../lib/layout';
import { addEntity, addRelationship, layerHex, nodeSize, toFlow, withPositions } from '../../lib/model';
import { Spinner } from '../common';

const nodeTypes = { entity: EntityNode };
const edgeTypes = { relationship: RelationshipEdge };
const motion = (duration) => window.matchMedia('(prefers-reduced-motion: reduce)').matches ? 0 : duration;

function structuralSig(model) {
  const ents = model.entities.map((e) =>
    `${e.id}~${e.name}~${e.schema || ''}~${e.layer}~` +
    (e.columns || []).map((c) => `${c.name}:${c.type}:${c.is_primary_key ? 'P' : ''}${c.is_foreign_key ? 'F' : ''}`).join(',')
  ).join('|');
  const rels = (model.relationships || []).map((r) => `${r.id}:${r.from_entity}>${r.to_entity}:${r.type}`).join('|');
  const lin = (model.lineage || []).map((l) => `${l.source}>${l.target}`).join('|');
  return `${model.name}#${model.model_type}#${ents}##${rels}##${lin}`;
}

function Flow({ model, onModelChange, selection, onSelect, active = true, autoLayoutSignal, onInferKeys, inferringKeys, onSample, onAssistant }) {
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const rf = useReactFlow();
  const wrapRef = useRef(null);
  const pendingFit = useRef(false);
  const sig = useMemo(() => structuralSig(model), [model]);
  const prevSig = useRef(null);

  // Rebuild nodes/edges only on structural change (drag/position edits don't trigger).
  useEffect(() => {
    if (prevSig.current === sig) return;
    prevSig.current = sig;
    const { nodes: n, edges: e } = toFlow(model);
    setNodes(n);
    setEdges(e);
  }, [sig, model, setNodes, setEdges]);

  // Auto-layout when a fresh model is loaded (signal bumps).
  useEffect(() => {
    if (!autoLayoutSignal) return;
    const base = toFlow(model);
    if (!base.nodes.length) return;
    const laid = model.source?.kind === 'sample' ? base.nodes : autoLayout(base.nodes, base.edges);
    setNodes(laid);
    setEdges(base.edges);
    prevSig.current = sig;
    onModelChange(withPositions(model, laid), true);
    // a hidden canvas has no size to fit to — fit once it's shown again
    if (!active) { pendingFit.current = true; return undefined; }
    const t = setTimeout(() => rf.fitView({ padding: 0.18, duration: motion(320), maxZoom: 1 }), 80);
    return () => clearTimeout(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [autoLayoutSignal]);

  useEffect(() => {
    if (!active || !pendingFit.current) return undefined;
    pendingFit.current = false;
    const t = setTimeout(() => rf.fitView({ padding: 0.18, maxZoom: 1 }), 60);
    return () => clearTimeout(t);
  }, [active, rf]);

  const displayNodes = useMemo(
    () => nodes.map((n) => ({ ...n, selected: selection?.type === 'entity' && selection.id === n.id })),
    [nodes, selection]
  );
  const displayEdges = useMemo(() => {
    const byId = new Map(nodes.map((node) => [node.id, node]));
    return edges.map((e) => {
      const sel = selection?.type === 'relationship' && selection.id === e.id;
      const source = byId.get(e.source), target = byId.get(e.target);
      if (!source || !target) return { ...e, selected: sel, animated: false };
      // Re-evaluate facing handles while dragging, not just when loading a model.
      // Vertically aligned tables share the outside edge so links avoid their bodies.
      const sourceRight = source.position.x + (source.measured?.width || source.width || 264);
      const targetRight = target.position.x + (target.measured?.width || target.width || 264);
      const sourceHandle = sourceRight <= target.position.x ? 'r' : 'l';
      const targetHandle = targetRight <= source.position.x ? 'r' : 'l';
      return { ...e, sourceHandle, targetHandle, selected: sel, animated: false };
    });
  }, [edges, nodes, selection]);

  const onConnect = useCallback((params) => {
    const { model: m, id } = addRelationship(model, params.source, params.target);
    onModelChange(m);
    if (id) onSelect({ type: 'relationship', id });
  }, [model, onModelChange, onSelect]);

  const onNodeDragStop = useCallback(() => {
    onModelChange(withPositions(model, nodes), true);
  }, [model, nodes, onModelChange]);

  const doLayout = useCallback(() => {
    const laid = autoLayout(nodes, edges);
    setNodes(laid);
    onModelChange(withPositions(model, laid), true);
    setTimeout(() => rf.fitView({ padding: 0.18, duration: motion(320), maxZoom: 1 }), 60);
  }, [nodes, edges, model, onModelChange, rf, setNodes]);

  // A cleared canvas starts at 100% zoom, not at the previous diagram's zoom.
  const isEmpty = !model.entities.length;
  useEffect(() => {
    if (isEmpty) rf.setViewport({ x: 0, y: 0, zoom: 1 });
  }, [isEmpty, rf]);

  const doAddEntity = useCallback(() => {
    const box = wrapRef.current?.getBoundingClientRect();
    const c = rf.screenToFlowPosition
      ? rf.screenToFlowPosition(box ? { x: box.left + box.width / 2, y: box.top + box.height / 2 }
        : { x: window.innerWidth / 2, y: window.innerHeight / 2 })
      : { x: 80, y: 80 };
    // never drop a new table on top of another one: step right past any overlap,
    // leaving room between neighbours for a link's label and its 1 / ∞ ends
    const W = 264, H = 120, GAP = 140;
    const overlaps = (x, y) => model.entities.some((e) => {
      if (!e.position) return false;
      const sz = nodeSize(e);
      return x < e.position.x + sz.width + GAP && x + W + GAP > e.position.x
        && y < e.position.y + sz.height + GAP && y + H + GAP > e.position.y;
    });
    let x = c.x - W / 2, y = c.y - H / 2;
    for (let i = 0; i < 60 && overlaps(x, y); i++) x += W + GAP;
    const { model: m, id } = addEntity(model, { position: { x, y } });
    onModelChange(m);
    onSelect({ type: 'entity', id });
    rf.setCenter(x + W / 2, y + H / 2, { zoom: Math.min(Math.max(rf.getZoom(), 0.6), 1), duration: motion(250) });
  }, [model, onModelChange, onSelect, rf]);

  useEffect(() => {
    if (selection?.type !== 'entity' || !selection.focus) return;
    const timer = setTimeout(() => rf.fitView({ nodes: [{ id: selection.id }], padding: 0.65, maxZoom: 1, duration: motion(280) }), 60);
    return () => clearTimeout(timer);
  }, [selection, rf]);

  useEffect(() => {
    if (!active) return undefined;
    const keydown = (event) => {
      if (event.target.closest('input, textarea, select, [contenteditable="true"], dialog') || event.metaKey || event.ctrlKey || event.altKey) return;
      const key = event.key.toLowerCase();
      if (key === 'n') { event.preventDefault(); doAddEntity(); }
      if (key === 'l' && nodes.length) { event.preventDefault(); doLayout(); }
      if (key === '0' && nodes.length) { event.preventDefault(); rf.fitView({ padding: 0.18, duration: motion(320), maxZoom: 1 }); }
    };
    window.addEventListener('keydown', keydown);
    return () => window.removeEventListener('keydown', keydown);
  }, [active, doAddEntity, doLayout, nodes.length, rf]);

  if (!model.entities.length) {
    return (
      <div className="canvas-wrap" ref={wrapRef}>
        <ReactFlow nodes={[]} edges={[]} proOptions={{ hideAttribution: true }} deleteKeyCode={null}>
          <Background variant={BackgroundVariant.Dots} gap={22} size={1} color="var(--canvas-dot)" />
        </ReactFlow>
        <div className="canvas-empty">
          <div className="canvas-empty-content">
            <span className="canvas-empty-art"><Icon name="model" /></span>
            <h2>A clear canvas. A fresh start.</h2>
            <p>Bring in a schema, build from raw files, or add your first table. Your next model starts here.</p>
            <div className="canvas-empty-actions">
              <button className="btn btn-ink" onClick={doAddEntity}><Icon name="plus" className="icon-sm" /> Add your first table</button>
              {onSample && <button className="btn" onClick={onSample}>Try a sample</button>}
            </div>
            <span className="canvas-empty-caption">A table is just the beginning. Connect the dots as you go.</span>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="canvas-wrap" ref={wrapRef}>
      <ReactFlow
        nodes={displayNodes}
        edges={displayEdges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        onNodeDragStop={onNodeDragStop}
        onNodeClick={(_, n) => onSelect({ type: 'entity', id: n.id })}
        onEdgeClick={(_, e) => { if (!e.data?.lineage) onSelect({ type: 'relationship', id: e.id }); }}
        onPaneClick={() => onSelect(null)}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        connectionMode={ConnectionMode.Loose}
        minZoom={0.05}
        maxZoom={2.2}
        fitView
        fitViewOptions={{ padding: 0.18, maxZoom: 1 }}
        deleteKeyCode={null}
        proOptions={{ hideAttribution: true }}
        defaultEdgeOptions={{ type: 'smoothstep' }}
      >
        <Background variant={BackgroundVariant.Dots} gap={22} size={1} color="var(--canvas-dot)" />
        <Controls showInteractive={false} />
        <MiniMap
          pannable zoomable
          nodeColor={(n) => layerHex(n.data?.entity?.layer)}
          nodeStrokeWidth={0}
          maskColor="color-mix(in srgb, var(--canvas-bg) 80%, transparent)"
          style={{ width: 132, height: 90 }}
          ariaLabel="Model overview. Drag to pan, scroll to zoom."
        />
      </ReactFlow>

      <div className="canvas-toolbar">
        <button className="btn btn-ink btn-sm" onClick={doAddEntity} title="Add table (N)" aria-keyshortcuts="N">
          <Icon name="plus" className="icon-sm" /> Add table
        </button>
        <span className="sep" aria-hidden="true" />
        <button className="btn btn-ghost btn-sm" onClick={doLayout} title="Arrange tables (L)" aria-label="Arrange tables" aria-keyshortcuts="L">
          <Icon name="layers" className="icon-sm" /><span className="toolbar-label-optional">Arrange</span>
        </button>
        <button className="btn btn-ghost btn-icon" onClick={() => rf.fitView({ padding: 0.18, duration: motion(320), maxZoom: 1 })} title="Fit to view (0)" aria-label="Fit to view" aria-keyshortcuts="0">
          <Icon name="fit" />
        </button>
        <span className="sep" aria-hidden="true" />
        {onInferKeys && (
          <button className="btn btn-ghost btn-sm" onClick={onInferKeys} disabled={inferringKeys}
            title="Infer missing primary and foreign keys with AI" aria-label="Infer keys with AI">
            {inferringKeys ? <Spinner /> : <Icon name="key" className="icon-sm" />}<span className="toolbar-label-optional">{inferringKeys ? 'Finding keys…' : 'Infer keys'}</span>
          </button>
        )}
        {onAssistant && <button className="btn btn-ghost btn-icon" onClick={onAssistant} title="Open AI assistant" aria-label="Open AI assistant"><Icon name="sparkles" /></button>}
      </div>
      <div className="canvas-hint">Drag handles to connect tables. Select a table to edit.</div>
    </div>
  );
}

export default function ERCanvas(props) {
  return (
    <ReactFlowProvider>
      <Flow {...props} />
    </ReactFlowProvider>
  );
}
