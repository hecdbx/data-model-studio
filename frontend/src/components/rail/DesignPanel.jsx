import React, { useEffect, useMemo, useRef, useState } from 'react';
import { api } from '../../lib/api';
import { Icon } from '../../lib/icons';
import { InlineError, Spinner, useToast } from '../common';

const MODEL_TYPES = [
  { id: 'medallion', label: 'Medallion', hint: 'Bronze, silver, and gold layers', icon: 'layers' },
  { id: 'star_schema', label: 'Star schema', hint: 'Facts and dimensions', icon: 'sparkles' },
  { id: 'data_vault', label: 'Data Vault', hint: 'Hubs, links, and satellites', icon: 'gitBranch' },
];

export default function DesignPanel({ onLoadModel, onConfirmReplace, onStartBlank, model, selection, onSelect, onClose }) {
  const toast = useToast();
  const [showVolume, setShowVolume] = useState(false);
  const [catalogs, setCatalogs] = useState(null);
  const [catalog, setCatalog] = useState('');
  const [schemas, setSchemas] = useState(null);
  const [schema, setSchema] = useState('');
  const [volumes, setVolumes] = useState(null);
  const [volume, setVolume] = useState('');
  const [errors, setErrors] = useState({});
  const [attempt, setAttempt] = useState(0);
  const [modelType, setModelType] = useState('medallion');
  const [busy, setBusy] = useState(false);
  const [tableQuery, setTableQuery] = useState('');
  const generation = useRef(0);
  const tables = useMemo(() => model.entities.filter((entity) => entity.name.toLowerCase().includes(tableQuery.toLowerCase())), [model.entities, tableQuery]);

  useEffect(() => () => { generation.current += 1; }, []);
  useEffect(() => {
    if (!showVolume) return;
    let active = true;
    setCatalogs(null); setErrors((previous) => ({ ...previous, catalog: '' }));
    api.catalogs().then((data) => { if (active) setCatalogs(data.catalogs || []); })
      .catch((error) => { if (active) setErrors((previous) => ({ ...previous, catalog: error.message })); });
    return () => { active = false; };
  }, [showVolume, attempt]);
  useEffect(() => {
    let active = true;
    setSchema(''); setSchemas(null); setVolume(''); setVolumes(null);
    setErrors((previous) => ({ ...previous, schema: '', volume: '' }));
    if (catalog) api.schemas(catalog).then((data) => { if (active) setSchemas(data.schemas || []); })
      .catch((error) => { if (active) setErrors((previous) => ({ ...previous, schema: error.message })); });
    return () => { active = false; };
  }, [catalog, attempt]);
  useEffect(() => {
    let active = true;
    setVolume(''); setVolumes(null); setErrors((previous) => ({ ...previous, volume: '' }));
    if (catalog && schema) api.volumes(catalog, schema).then((data) => { if (active) setVolumes(data.volumes || []); })
      .catch((error) => { if (active) setErrors((previous) => ({ ...previous, volume: error.message })); });
    return () => { active = false; };
  }, [catalog, schema, attempt]);

  const generate = async () => {
    if (!(catalog && schema && volume) || busy) return;
    const current = ++generation.current;
    setBusy(true);
    try {
      const { job_id } = await api.generateFromVolume({ catalog, schema_name: schema, volume, model_type: modelType });
      for (let i = 0; i < 240; i++) {
        await new Promise((resolve) => setTimeout(resolve, 2500));
        if (current !== generation.current) return;
        const job = await api.agentJob(job_id);
        if (current !== generation.current) return;
        if (job.status === 'done') {
          onLoadModel(job.model);
          toast(`Model ready. ${job.model.entities?.length || 0} tables added to the canvas.`);
          return;
        }
        if (job.status === 'error') throw new Error(job.error || 'The model could not be generated. Please try again.');
      }
      throw new Error('Model generation is taking longer than expected. Please try again in a few minutes.');
    } catch (error) { if (current === generation.current) toast(error.message, 'error'); }
    finally { if (current === generation.current) setBusy(false); }
  };

  const sourceError = errors.catalog || errors.schema || errors.volume;
  const changePattern = (event) => {
    if (!['ArrowDown', 'ArrowRight', 'ArrowUp', 'ArrowLeft', 'Home', 'End'].includes(event.key) || busy) return;
    event.preventDefault();
    const index = MODEL_TYPES.findIndex((pattern) => pattern.id === modelType);
    const next = event.key === 'Home' ? 0 : event.key === 'End' ? MODEL_TYPES.length - 1
      : (index + (['ArrowDown', 'ArrowRight'].includes(event.key) ? 1 : MODEL_TYPES.length - 1)) % MODEL_TYPES.length;
    setModelType(MODEL_TYPES[next].id);
    document.getElementById(`pattern-${MODEL_TYPES[next].id}`)?.focus();
  };
  return (
    <>
      <div className="panel-head">
        <span className="panel-head-icon"><Icon name="model" /></span>
        <div className="panel-head-copy"><h2>Model explorer</h2><p>{model.entities.length ? 'Everything in your model' : 'Give your ideas a structure'}</p></div>
        <button className="btn btn-ghost btn-icon panel-close" onClick={onClose} aria-label="Close sources"><Icon name="x" /></button>
      </div>
      <div className="panel-body">
        <button className="entry-card source-new" onClick={onStartBlank}>
          <span className="ec-badge"><Icon name="plus" /></span>
          <span className="entry-copy"><strong>{model.entities.length ? 'New blank model' : 'Start with a blank canvas'}</strong><span>Add tables and connect them by hand.</span></span>
        </button>
        {model.entities.length > 0 && <section aria-label="Tables in the model">
          <div className="section-label"><span>Tables</span><span>{model.entities.length}</span></div>
          {model.entities.length > 5 && <div className="search"><Icon name="search" className="icon-sm" /><input className="input" aria-label="Find a table" placeholder="Find a table…" value={tableQuery} onChange={(event) => setTableQuery(event.target.value)} /></div>}
          <div className="list" style={{ maxHeight: 300, overflowY: 'auto' }}>
            {tables.map((entity) => <button key={entity.id} className="list-row overview-table" style={{ '--layer': `var(--layer-${entity.layer || 'table'}, var(--layer-table))` }}
              aria-pressed={selection?.type === 'entity' && selection.id === entity.id} onClick={() => onSelect({ type: 'entity', id: entity.id, focus: true })}>
              <Icon name="table" /><span className="row-title" title={entity.name}>{entity.name}</span><span className="row-meta">{entity.columns.length}</span>
            </button>)}
            {!tables.length && <p className="field-hint">No matching tables. Try a different search.</p>}
          </div>
        </section>}
        <section className="section-divider">
          <button className="disclosure-button" aria-expanded={showVolume} aria-controls="volume-source" onClick={() => setShowVolume((value) => !value)} disabled={busy}>
            <Icon name={showVolume ? 'chevronDown' : 'chevronRight'} /><span>Build from raw files</span><Icon name="sparkles" className="icon-sm" />
          </button>
          <p className="disclosure-hint">Let AI turn a volume into a model</p>
          {showVolume && <div id="volume-source" className="disclosure-content">
            <div className="field">
              <label htmlFor="design-catalog">Catalog</label>
              <select id="design-catalog" className="select" value={catalog} disabled={!catalogs || busy} onChange={(event) => setCatalog(event.target.value)}>
                <option value="">{errors.catalog ? 'Catalogs unavailable' : !catalogs ? 'Loading catalogs…' : !catalogs.length ? 'No catalogs available' : 'Choose a catalog'}</option>
                {catalogs?.map((item) => <option key={item.name} value={item.name}>{item.name}</option>)}
              </select>
            </div>
            <div className="field">
              <label htmlFor="design-schema">Schema</label>
              <select id="design-schema" className="select" value={schema} disabled={!schemas || busy} onChange={(event) => setSchema(event.target.value)}>
                <option value="">{catalog && !schemas && !errors.schema ? 'Loading schemas…' : schemas && !schemas.length ? 'No schemas available' : 'Choose a schema'}</option>
                {schemas?.map((item) => <option key={item.name} value={item.name}>{item.name}</option>)}
              </select>
            </div>
            <div className="field">
              <label htmlFor="design-volume">Volume</label>
              <select id="design-volume" className="select" value={volume} disabled={!volumes || busy} onChange={(event) => setVolume(event.target.value)}>
                <option value="">{schema && !volumes && !errors.volume ? 'Loading volumes…' : volumes && !volumes.length ? 'No volumes available' : 'Choose a volume'}</option>
                {volumes?.map((item) => <option key={item.name} value={item.name}>{item.name}</option>)}
              </select>
            </div>
            {sourceError && <InlineError title="Couldn’t load this source" message="Check workspace access and try again." onRetry={() => setAttempt((value) => value + 1)} />}
            <div className="section-divider">
              <div className="section-label" id="pattern-label"><span>Model pattern</span></div>
              <div className="list pattern-list" role="radiogroup" aria-labelledby="pattern-label" onKeyDown={changePattern}>
                {MODEL_TYPES.map((pattern) => <button key={pattern.id} id={`pattern-${pattern.id}`} className="list-row pattern-option" role="radio" aria-checked={modelType === pattern.id} tabIndex={modelType === pattern.id ? 0 : -1} disabled={busy} onClick={() => setModelType(pattern.id)}>
                  <span className="pattern-symbol"><Icon name={pattern.icon} /></span>
                  <span className="row-title"><strong>{pattern.label}</strong><small>{pattern.hint}</small></span><span className="radio-indicator" />
                </button>)}
              </div>
            </div>
          </div>}
        </section>
      </div>
      <div className="panel-foot">
        {showVolume ? <>
          <p className="panel-foot-note" role="status">{busy ? 'Reading files and designing your model…' : 'Choose a volume and a model pattern'}</p>
          <button className="btn btn-primary btn-block" disabled={!(catalog && schema && volume) || busy} onClick={() => onConfirmReplace('Generate model', generate)}>
            {busy ? <><Spinner /> Designing your model…</> : <><Icon name="sparkles" className="icon-sm" /> Generate model</>}
          </button>
        </> : <p className="field-hint">Start with a table, connect its relationships, and let the bigger picture take shape.</p>}
      </div>
    </>
  );
}
