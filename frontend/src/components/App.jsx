import React, { lazy, Suspense, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { api } from '../lib/api';
import { Icon } from '../lib/icons';
import { emptyModel, findEntity, inferCardinality, mergeModel, placeNewEntities, stats, uid } from '../lib/model';
import { createSampleModel } from '../lib/sampleModel';
import { download, InlineError, Modal, PanelDivider, Spinner, Tabs, useToast } from './common';
import TopBar from './TopBar';
import Landing from './Landing';
import Inspector from './Inspector';
import Assistant from './dock/Assistant';
import CodePanel from './dock/CodePanel';
import ExplorePanel from './rail/ExplorePanel';
import DesignPanel from './rail/DesignPanel';

const ERCanvas = lazy(() => import('./canvas/ERCanvas'));

function normalize(m) {
  const model = JSON.parse(JSON.stringify(m || {}));
  model.model_type ||= 'physical';
  model.name ||= 'model';
  model.source ||= { kind: 'blank' };
  model.entities = (model.entities || []).map((e) => {
    if (!e.id) e.id = e.schema ? `${e.schema}.${e.name}` : e.name || uid('ent');
    e.columns = e.columns || [];
    if (!e.primary_key) e.primary_key = e.columns.filter((c) => c.is_primary_key).map((c) => c.name);
    return e;
  });
  model.relationships = (model.relationships || []).map((r) => ({
    ...r, id: r.id || uid('rel'), type: r.type || inferCardinality(findEntity(model, r.from_entity), r.from_columns),
  }));
  model.pipelines = model.pipelines || [];
  model.lineage = model.lineage || [];
  return model;
}

function SetupModal({ setup, onClose, onProvision, provisioning }) {
  const row = (ok, label, detail) => (
    <div className={`setup-row ${ok ? 'ok' : ''}`}>
      <Icon name={ok ? 'check' : 'alert'} />
      <div><strong>{label}</strong><small>{detail || 'Not configured'}</small></div>
    </div>
  );
  return (
    <Modal title="Workspace setup" icon="settings" onClose={onClose}
      description="Connect the resources used to explore data, save generated code, and apply your models.">
      {setup?.connection_error && <InlineError title="Workspace connection unavailable" message="Check the app’s connection and permissions, then retry setup." />}
      <div className="setup-list">
        {row(setup?.schema_exists, 'Working schema', [setup?.catalog, setup?.schema].filter(Boolean).join('.'))}
        {row(setup?.volume_exists, 'Artifacts volume', setup?.volume)}
        {row(setup?.job_exists, 'Apply job', setup?.job_exists ? `Job #${setup.job_id}` : 'Deploy the app bundle to create this job')}
        {row(!!setup?.warehouse_id, 'SQL warehouse', setup?.warehouse_id || setup?.warehouse_error)}
      </div>
      <p className="field-hint">Workspace operations use the app’s configured permissions.</p>
      <button className="btn btn-primary btn-block" onClick={onProvision} disabled={provisioning}>
        {provisioning ? <><Spinner /> Setting up resources…</> : <><Icon name="refresh" className="icon-sm" /> Run workspace setup</>}
      </button>
    </Modal>
  );
}

const DOCK_TABS = [
  { id: 'inspector', label: 'Properties', icon: 'settings' },
  { id: 'assistant', label: 'Assistant', icon: 'sparkles' },
  { id: 'code', label: 'Code', icon: 'code' },
];

function ShortcutsModal({ onClose }) {
  const shortcuts = [['Add a table', 'N'], ['Fit model to view', '0'], ['Arrange tables', 'L'], ['Focus canvas', 'F'], ['Clear selection', 'Esc'], ['Show keyboard shortcuts', '?']];
  return <Modal title="A few useful shortcuts" description="Keep your hands on the keyboard while working on the canvas." icon="help" onClose={onClose}>
    <div className="shortcut-list">{shortcuts.map(([label, key]) => <div key={key} className="shortcut-row"><span>{label}</span><kbd>{key}</kbd></div>)}</div>
    <p className="field-hint">In the assistant, Enter sends your message and Shift + Enter starts a new line.</p>
  </Modal>;
}

function initialTheme() {
  try { return localStorage.getItem('dms-theme') === 'dark' ? 'dark' : 'light'; }
  catch { return 'light'; }
}

// What replacing the canvas would lose — positions alone don't count as work.
const workSig = (m) => JSON.stringify([m.name, m.entities.map(({ position, ...e }) => e), m.relationships, m.lineage]);
const plural = (n, word) => `${word}${n === 1 ? '' : 's'}`;

export default function App() {
  const toast = useToast();
  const [theme, setTheme] = useState(initialTheme);
  const [mode, setMode] = useState('landing');
  const [model, setModel] = useState(() => emptyModel());
  const [baseModel, setBaseModel] = useState(null);
  const [selection, setSelection] = useState(null);
  const [dockTab, setDockTab] = useState('inspector');
  const [railHidden, setRailHidden] = useState(() => window.innerWidth <= 1100);
  const [dockHidden, setDockHidden] = useState(() => window.innerWidth <= 900);
  const [railWidth, setRailWidth] = useState(284);
  const [dockWidth, setDockWidth] = useState(360);
  const [shortcutsOpen, setShortcutsOpen] = useState(false);
  const [pendingReplace, setPendingReplace] = useState(null);
  // modes opened so far stay mounted, so a running generation, the conversation,
  // and source selections survive a trip home or to the other mode
  const [visited, setVisited] = useState(() => new Set());
  const lastWorkspace = useRef('design');
  const focusPanels = useRef({ railHidden: false, dockHidden: false });
  const [layoutSignal, setLayoutSignal] = useState(0);
  const [attached, setAttached] = useState([]);
  const [setup, setSetup] = useState(null);
  const [setupOpen, setSetupOpen] = useState(false);
  const [provisioning, setProvisioning] = useState(false);
  const [inferringKeys, setInferringKeys] = useState(false);

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme);
    try { localStorage.setItem('dms-theme', theme); } catch { /* Storage can be unavailable in private browsing. */ }
  }, [theme]);
  useEffect(() => { api.bootstrapStatus().then(setSetup).catch((error) => setSetup({ initialized: false, connection_error: error.message })); }, []);
  useEffect(() => {
    const media = window.matchMedia('(max-width: 900px)');
    const sourcesMedia = window.matchMedia('(max-width: 1100px)');
    const collapse = () => { if (media.matches) { setRailHidden(true); setDockHidden(true); } };
    const collapseSources = () => { if (sourcesMedia.matches) setRailHidden(true); };
    const fitPanels = () => {
      setRailWidth((value) => Math.min(value, Math.max(240, Math.floor(window.innerWidth * 0.28))));
      setDockWidth((value) => Math.min(value, Math.max(320, Math.floor(window.innerWidth * 0.36))));
    };
    media.addEventListener('change', collapse);
    sourcesMedia.addEventListener('change', collapseSources);
    window.addEventListener('resize', fitPanels);
    return () => {
      media.removeEventListener('change', collapse);
      sourcesMedia.removeEventListener('change', collapseSources);
      window.removeEventListener('resize', fitPanels);
    };
  }, []);

  const toggleRail = () => {
    if (railHidden && window.innerWidth <= 1100) setDockHidden(true);
    setRailHidden((value) => !value);
  };
  const toggleDock = () => {
    if (dockHidden && window.innerWidth <= 1100) setRailHidden(true);
    setDockHidden((value) => !value);
  };
  const toggleFocus = useCallback(() => {
    if (railHidden && dockHidden) {
      setRailHidden(window.innerWidth <= 1100 ? true : focusPanels.current.railHidden);
      setDockHidden(window.innerWidth <= 1100 ? false : focusPanels.current.dockHidden);
    } else {
      focusPanels.current = { railHidden, dockHidden };
      setRailHidden(true); setDockHidden(true);
    }
  }, [railHidden, dockHidden]);

  useEffect(() => {
    const keydown = (event) => {
      if (event.target.closest('input, textarea, select, [contenteditable="true"], dialog') || event.metaKey || event.ctrlKey || event.altKey) return;
      if (event.key === '?') { event.preventDefault(); setShortcutsOpen(true); }
      if (mode === 'landing') return;
      if (event.key.toLowerCase() === 'f') { event.preventDefault(); toggleFocus(); }
      if (event.key === 'Escape') {
        setSelection(null);
        if (window.innerWidth <= 900) { setRailHidden(true); setDockHidden(true); }
      }
    };
    window.addEventListener('keydown', keydown);
    return () => window.removeEventListener('keydown', keydown);
  }, [mode, toggleFocus]);

  const attachedIds = useMemo(() => new Set(attached.map((a) => a.pipeline_id)), [attached]);
  const pipelineSource = useMemo(() => attached.map((a) => a.source_code).filter(Boolean).join('\n\n'), [attached]);

  const target = useMemo(() => {
    const cat = model.source?.catalog || setup?.catalog;
    const sch = (model.source?.schemas && model.source.schemas[0]) || setup?.schema;
    return { catalog: cat, schema: sch };
  }, [model, setup]);

  const loadModel = useCallback((m) => {
    const nm = normalize(m);
    setModel(nm);
    setBaseModel(nm.source?.kind === 'uc_schemas' ? JSON.parse(JSON.stringify(nm)) : null);
    setAttached([]);
    setSelection(null);
    setLayoutSignal((s) => s + 1);
    setDockTab('inspector');
  }, []);

  const applyAgentModel = useCallback((m) => {
    // merge (never replace): tables the agent didn't mention stay on the canvas
    const nm = normalize(mergeModel(model, m || {}));
    setSelection(null);
    if (nm.entities.some((e) => e.position)) setModel(placeNewEntities(nm));
    else { setModel(nm); setLayoutSignal((s) => s + 1); }
  }, [model]);

  const startBlank = useCallback(() => {
    const m = emptyModel();
    setModel(m); setBaseModel(null); setAttached([]); setSelection(null);
    toast('Blank canvas ready — add a table to begin.');
  }, [toast]);

  const attachPipeline = useCallback((overlay) => {
    setAttached((prev) => prev.some((a) => a.pipeline_id === overlay.pipeline_id) ? prev : [...prev, overlay]);
    const m = JSON.parse(JSON.stringify(model));
    if (!Array.isArray(m.lineage)) m.lineage = [];
    if (!Array.isArray(m.pipelines)) m.pipelines = [];
    if (!Array.isArray(m.relationships)) m.relationships = [];
    const tableset = new Set(overlay.tables || []);
    m.entities.forEach((e) => { if (tableset.has(e.name)) e.source_pipeline = overlay.pipeline_id; });
    const onCanvas = new Set(m.entities.map((e) => e.name));
    let newLineage = 0;
    (overlay.lineage || []).forEach((l) => {
      if (!m.lineage.some((x) => x.source === l.source && x.target === l.target)) {
        m.lineage.push(l);
        if (onCanvas.has(l.source) && onCanvas.has(l.target)) newLineage += 1;
      }
    });
    if (!m.pipelines.some((p) => p.pipeline_id === overlay.pipeline_id)) {
      m.pipelines.push({ pipeline_id: overlay.pipeline_id, name: overlay.name, tables: overlay.tables, target: overlay.target });
    }
    // FK relationships inferred from the pipeline's joins, already resolved onto
    // the canvas entities (columns validated) by the backend. Skip any table pair
    // the diagram already links (e.g. a declared UC constraint).
    const byId = {};
    m.entities.forEach((e) => { byId[e.id] = e; });
    const pairKey = (fe, te) => `${fe}|${te}`;
    const fullKey = (r) => `${r.from_entity}|${(r.from_columns || []).join(',')}|${r.to_entity}`;
    const linkedPairs = new Set(m.relationships.map((r) => pairKey(r.from_entity, r.to_entity)));
    const seen = new Set(m.relationships.map(fullKey));
    let added = 0;
    (overlay.relationships || []).forEach((r) => {
      const child = byId[r.from_entity];
      const parent = byId[r.to_entity];
      if (!child || !parent || child.id === parent.id || seen.has(fullKey(r))) return;
      // a pair already linked by an earlier source keeps its link; several links
      // between one pair only come from the pipeline itself (role-playing joins)
      if (linkedPairs.has(pairKey(child.id, parent.id)) && !(overlay.relationships || []).some(
        (o) => o !== r && o.from_entity === r.from_entity && o.to_entity === r.to_entity)) return;
      seen.add(fullKey(r));
      m.relationships.push({
        id: uid('rel'), from_entity: child.id, from_columns: r.from_columns || [],
        to_entity: parent.id, to_columns: r.to_columns || [], type: r.type || inferCardinality(child, r.from_columns),
        description: r.description || 'Inferred from pipeline join', inferred: 'pipeline',
      });
      (r.from_columns || []).forEach((fc, i) => {
        const col = (child.columns || []).find((c) => c.name === fc);
        if (col) { col.is_foreign_key = true; col.references = { entity: parent.id, column: (r.to_columns || [])[i] || (r.to_columns || [])[0] || '' }; }
      });
      added += 1;
    });
    setModel(m);
    // new links change the graph's shape — re-run the layout so it reads
    // left-to-right (bronze → silver → gold) instead of a tangle over the old grid
    if (added || newLineage) setLayoutSignal((s) => s + 1);
    const nTables = overlay.tables?.length || 0;
    const readable = nTables > 0 || !!(overlay.source_code && overlay.source_code.length);
    if (readable) {
      toast(`Attached "${overlay.name}" — ${nTables} tables, ${added} relationship${added === 1 ? '' : 's'} inferred.`);
    } else {
      toast(`Attached "${overlay.name}" — couldn't read its source. Reload the app to authorize workspace access, or use “Paste pipeline source instead”.`);
    }
  }, [model, toast]);

  const runInferKeys = useCallback(async () => {
    if (!model.entities.length) { toast('Add or load some tables first.', 'error'); return; }
    setInferringKeys(true);
    try {
      const { job_id } = await api.inferKeys(model);
      let done = null;
      // Poll up to ~5 min; the backend keeps working even across a long reasoning pass.
      for (let i = 0; i < 150; i++) {
        await new Promise((r) => setTimeout(r, 2000));
        const job = await api.agentJob(job_id);
        if (job.status === 'done') { done = job; break; }
        if (job.status === 'error') throw new Error(job.error || 'Agent error');
      }
      if (!done) throw new Error('Still inferring keys — the model may be large. Give it a moment and click “Infer keys” again.');
      applyAgentModel(done.model);
      const nRel = (done.model.relationships || []).length;
      toast(`AI inferred keys — ${nRel} relationship${nRel === 1 ? '' : 's'} on the diagram.`);
    } catch (e) { toast(e.message, 'error'); }
    finally { setInferringKeys(false); }
  }, [model, applyAgentModel, toast]);

  const runProvision = async () => {
    setProvisioning(true);
    try { const res = await api.provision(); setSetup(res.status); toast('Setup complete.'); }
    catch (e) { toast(e.message, 'error'); }
    finally { setProvisioning(false); }
  };

  const chooseMode = (nextMode) => {
    if (nextMode !== 'landing') {
      lastWorkspace.current = nextMode;
      setVisited((seen) => seen.has(nextMode) ? seen : new Set(seen).add(nextMode));
    }
    setMode(nextMode);
  };

  const selectItem = (next) => {
    setSelection(next);
    if (next) {
      setDockTab('inspector');
      setDockHidden(false);
      if (window.innerWidth <= 1100) setRailHidden(true);
    }
  };

  const openTool = (tab) => {
    setDockTab(tab);
    setDockHidden(false);
    if (window.innerWidth <= 1100) setRailHidden(true);
  };

  const createModel = (kind) => {
    if (kind === 'sample') {
      loadModel(createSampleModel());
      chooseMode('design');
      toast('Sample model ready. Select a table to explore its properties.');
    } else {
      startBlank();
      chooseMode('design');
    }
  };
  // Ask before anything replaces the canvas, unless nothing would be lost (an
  // empty canvas, or a Unity Catalog model that can simply be loaded again).
  const confirmReplace = useCallback((label, run) => {
    const pristine = !model.entities.length || (baseModel && workSig(model) === workSig(baseModel));
    if (pristine) run();
    else setPendingReplace({ label, run });
  }, [model, baseModel]);
  const requestModel = (kind) => confirmReplace(kind === 'sample' ? 'Open sample' : 'Start blank', () => createModel(kind));
  const exportModel = () => {
    download(`${model.name || 'model'}.json`, JSON.stringify(model, null, 2));
    toast('Model exported.');
  };

  const counts = stats(model);
  const sourceLabel = ({ sample: 'Sample model', uc_schemas: 'Unity Catalog', volume: 'Volume source' })[model.source?.kind] || 'Local draft';
  const workspaceStyle = {
    '--rail-w': railHidden ? '0px' : `${railWidth}px`,
    '--dock-w': dockHidden ? '0px' : `${dockWidth}px`,
    '--left-divider': railHidden ? '0px' : '5px',
    '--right-divider': dockHidden ? '0px' : '5px',
  };

  return (
    <div className={`app ${mode === 'landing' ? 'app-home' : ''}`}>
      <TopBar mode={mode} onMode={chooseMode} theme={theme}
        onToggleTheme={() => setTheme((value) => value === 'dark' ? 'light' : 'dark')}
        setup={setup} onOpenSetup={() => setSetupOpen(true)}
        onToggleRail={toggleRail} onToggleDock={toggleDock} onToggleFocus={toggleFocus}
        railHidden={railHidden} dockHidden={dockHidden} />

      {mode === 'landing' && <Landing onMode={chooseMode} onSample={() => requestModel('sample')} hasModel={!!model.entities.length} onResume={() => chooseMode(lastWorkspace.current)} />}
      {visited.size > 0 && (
        <div className="workspace" style={workspaceStyle} hidden={mode === 'landing'}>
          {(!railHidden || !dockHidden) && <button className="panel-backdrop" aria-label="Close side panels" tabIndex={-1} onClick={() => { setRailHidden(true); setDockHidden(true); }} />}
          <aside id="sources-panel" className="rail" hidden={railHidden} aria-label={mode === 'explore' ? 'Data sources' : 'Model explorer'}>
            {visited.has('explore') && <div className="rail-view" hidden={mode !== 'explore'}>
              <ExplorePanel onLoadModel={loadModel} onConfirmReplace={confirmReplace} onAttachPipeline={attachPipeline} attachedIds={attachedIds} entities={model.entities} appPrincipal={setup?.app_principal} onClose={() => setRailHidden(true)} />
            </div>}
            {visited.has('design') && <div className="rail-view" hidden={mode !== 'design'}>
              <DesignPanel onLoadModel={loadModel} onConfirmReplace={confirmReplace} onStartBlank={() => requestModel('blank')} model={model} selection={selection} onSelect={selectItem} onClose={() => setRailHidden(true)} />
            </div>}
          </aside>
          <PanelDivider label="Resize sources panel" value={railWidth} min={240}
            max={Math.max(240, Math.min(400, window.innerWidth - (dockHidden ? 0 : dockWidth) - 370))}
            onChange={setRailWidth} hidden={railHidden} />

          <main className="canvas-pane" aria-label="Data model workspace">
            <div className="canvas-heading">
              <span className="model-heading-icon"><Icon name="model" /></span>
              <div className="model-title-group">
                <label htmlFor="model-name">Your workspace</label>
                <input id="model-name" className="model-title" aria-label="Model name" spellCheck={false} value={model.name}
                  onChange={(event) => setModel((current) => ({ ...current, name: event.target.value }))} />
              </div>
              <span className="model-source">{sourceLabel}</span>
              <div className="model-actions">
                <button className="btn btn-ghost btn-sm" onClick={() => openTool('code')} title="View generated code" aria-label="View generated code">
                  <Icon name="code" /><span className="action-label">View code</span>
                </button>
                <button className="btn btn-ghost btn-icon" aria-label="Export model as JSON" title="Export model as JSON" onClick={exportModel}><Icon name="download" /></button>
              </div>
            </div>
            <Suspense fallback={<div className="canvas-wrap"><div className="loading-line"><Spinner /> Opening your canvas…</div></div>}>
              <ERCanvas model={model} onModelChange={setModel} selection={selection} onSelect={selectItem} active={mode !== 'landing'}
                autoLayoutSignal={layoutSignal} onInferKeys={runInferKeys} inferringKeys={inferringKeys}
                onSample={() => requestModel('sample')} onAssistant={() => openTool('assistant')} />
            </Suspense>
            <footer className="canvas-status" aria-label="Model statistics">
              <span className="status-item"><Icon name="table" /><strong>{counts.entities}</strong> {plural(counts.entities, 'table')}</span>
              <span className="status-item"><Icon name="columns" /><strong>{counts.columns}</strong> {plural(counts.columns, 'column')}</span>
              <span className="status-item"><Icon name="link" /><strong>{counts.relationships}</strong> {plural(counts.relationships, 'link')}</span>
              <button className="status-help" onClick={() => setShortcutsOpen(true)} aria-label="Show keyboard shortcuts"><span>Shortcuts</span><kbd>?</kbd></button>
            </footer>
          </main>

          <PanelDivider label="Resize properties panel" value={dockWidth} min={320}
            max={Math.max(320, Math.min(520, window.innerWidth - (railHidden ? 0 : railWidth) - 370))}
            direction={-1} onChange={setDockWidth} hidden={dockHidden} />
          <aside id="tools-panel" className="dock" hidden={dockHidden} aria-label="Model properties and tools">
            <button className="btn btn-ghost btn-icon panel-close" onClick={() => setDockHidden(true)} aria-label="Close properties and tools"><Icon name="x" /></button>
            <Tabs tabs={DOCK_TABS} value={dockTab} onChange={setDockTab} label="Model tools" id="tools" />
            <div className="dock-panel">
              <div id="tools-panel-inspector" className="dock-tab-content" role="tabpanel" aria-labelledby="tools-tab-inspector" hidden={dockTab !== 'inspector'} tabIndex={0}>
                <Inspector model={model} selection={selection} onModelChange={setModel} onSelect={selectItem} />
              </div>
              <div id="tools-panel-assistant" className="dock-tab-content" role="tabpanel" aria-labelledby="tools-tab-assistant" hidden={dockTab !== 'assistant'} tabIndex={0}>
                <Assistant model={model} pipelineSource={pipelineSource} onApplyModel={applyAgentModel} active={dockTab === 'assistant' && !dockHidden && mode !== 'landing'} />
              </div>
              <div id="tools-panel-code" className="dock-tab-content" role="tabpanel" aria-labelledby="tools-tab-code" hidden={dockTab !== 'code'} tabIndex={0}>
                <CodePanel model={model} baseModel={baseModel} target={target} active={dockTab === 'code' && !dockHidden && mode !== 'landing'} />
              </div>
            </div>
          </aside>
        </div>
      )}

      {setupOpen && <SetupModal setup={setup} onClose={() => setSetupOpen(false)} onProvision={runProvision} provisioning={provisioning} />}
      {shortcutsOpen && <ShortcutsModal onClose={() => setShortcutsOpen(false)} />}
      {pendingReplace && <Modal title="Start a different model?" icon="model" onClose={() => setPendingReplace(null)}
        description={`Your current model has ${model.entities.length} ${plural(model.entities.length, 'table')}. Export a copy if you want to keep your work before replacing the canvas.`}>
        <div className="modal-actions">
          <button className="btn btn-ghost" onClick={() => setPendingReplace(null)}>Keep current</button>
          <button className="btn" onClick={exportModel}><Icon name="download" className="icon-sm" /> Export copy</button>
          <button className="btn btn-primary" onClick={() => { const { run } = pendingReplace; setPendingReplace(null); run(); }}>{pendingReplace.label}</button>
        </div>
      </Modal>}
    </div>
  );
}
