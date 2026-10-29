import React, { useEffect, useMemo, useState } from 'react';
import { api } from '../../lib/api';
import { Icon } from '../../lib/icons';
import { InlineError, Spinner, useToast } from '../common';

const slimEntities = (entities) => (entities || []).map((entity) => ({
  id: entity.id, name: entity.name, schema: entity.schema || null, primary_key: entity.primary_key || [],
  columns: (entity.columns || []).map((column) => ({ name: column.name, is_primary_key: !!column.is_primary_key })),
}));

export default function ExplorePanel({ onLoadModel, onConfirmReplace, onAttachPipeline, attachedIds, entities, appPrincipal, onClose }) {
  const toast = useToast();
  const [catalogs, setCatalogs] = useState(null);
  const [catalog, setCatalog] = useState('');
  const [catalogError, setCatalogError] = useState('');
  const [catalogAttempt, setCatalogAttempt] = useState(0);
  const [schemas, setSchemas] = useState(null);
  const [schemaQuery, setSchemaQuery] = useState('');
  const [schemaError, setSchemaError] = useState('');
  const [schemaAttempt, setSchemaAttempt] = useState(0);
  const [picked, setPicked] = useState(() => new Set());
  const [loadingSchemas, setLoadingSchemas] = useState(false);
  const [loadingModel, setLoadingModel] = useState(false);
  const [showPipes, setShowPipes] = useState(false);
  const [pipes, setPipes] = useState(null);
  const [pipeError, setPipeError] = useState('');
  const [pipeAttempt, setPipeAttempt] = useState(0);
  const [pipeQuery, setPipeQuery] = useState('');
  const [attaching, setAttaching] = useState('');
  const [pasteSrc, setPasteSrc] = useState('');
  const [pasteName, setPasteName] = useState('');
  const [parsing, setParsing] = useState(false);

  useEffect(() => {
    let active = true;
    setCatalogError(''); setCatalogs(null);
    api.catalogs().then((data) => { if (active) setCatalogs(data.catalogs || []); })
      .catch((error) => { if (active) { setCatalogError(error.message); setCatalogs([]); } });
    return () => { active = false; };
  }, [catalogAttempt]);

  useEffect(() => {
    let active = true;
    setPicked(new Set()); setSchemaQuery(''); setSchemaError(''); setSchemas(null);
    if (!catalog) { setLoadingSchemas(false); return; }
    setLoadingSchemas(true);
    api.schemas(catalog).then((data) => { if (active) setSchemas(data.schemas || []); })
      .catch((error) => { if (active) setSchemaError(error.message); })
      .finally(() => { if (active) setLoadingSchemas(false); });
    return () => { active = false; };
  }, [catalog, schemaAttempt]);

  useEffect(() => {
    if (!showPipes) return;
    let active = true;
    setPipeError(''); setPipes(null);
    api.pipelines().then((data) => { if (active) setPipes(data.pipelines || []); })
      .catch((error) => { if (active) setPipeError(error.message); });
    return () => { active = false; };
  }, [showPipes, pipeAttempt]);

  const filteredSchemas = useMemo(() => (schemas || []).filter((schema) => schema.name.toLowerCase().includes(schemaQuery.toLowerCase())), [schemas, schemaQuery]);
  const matchingPipes = useMemo(() => (pipes || []).filter((pipe) => pipe.name.toLowerCase().includes(pipeQuery.toLowerCase())), [pipes, pipeQuery]);
  const toggle = (name) => setPicked((previous) => {
    const next = new Set(previous);
    next.has(name) ? next.delete(name) : next.add(name);
    return next;
  });

  const loadDiagram = async () => {
    if (!picked.size) return;
    setLoadingModel(true);
    try {
      const { model } = await api.modelFromSchemas(catalog, [...picked]);
      if (!model.entities.length) toast('No tables found in the selected schemas.');
      else toast(`Loaded ${model.entities.length} tables from ${picked.size} schema${picked.size === 1 ? '' : 's'}.`);
      onLoadModel(model);
    } catch (error) { toast(error.message, 'error'); }
    finally { setLoadingModel(false); }
  };

  const attach = async (pipe) => {
    setAttaching(pipe.pipeline_id);
    try { onAttachPipeline(await api.attachPipeline(pipe.pipeline_id, slimEntities(entities))); }
    catch (error) { toast(error.message, 'error'); }
    finally { setAttaching(''); }
  };

  const attachPasted = async () => {
    if (!pasteSrc.trim()) return;
    setParsing(true);
    try {
      onAttachPipeline(await api.parsePipeline(pasteSrc, pasteName || 'Pasted pipeline', slimEntities(entities)));
      setPasteSrc('');
    } catch (error) { toast(error.message, 'error'); }
    finally { setParsing(false); }
  };

  return (
    <>
      <div className="panel-head">
        <span className="panel-head-icon"><Icon name="database" /></span>
        <div className="panel-head-copy"><h2>Data sources</h2><p>Explore Unity Catalog</p></div>
        <button className="btn btn-ghost btn-icon panel-close" onClick={onClose} aria-label="Close sources"><Icon name="x" /></button>
      </div>
      <div className="panel-body">
        <div className="field">
          <label htmlFor="explore-catalog">Catalog</label>
          <select id="explore-catalog" className="select" value={catalog} disabled={!catalogs || !!catalogError || loadingModel} onChange={(event) => setCatalog(event.target.value)}>
            <option value="">{!catalogs ? 'Loading catalogs…' : !catalogs.length && !catalogError ? 'No catalogs available' : 'Choose a catalog'}</option>
            {catalogs?.map((item) => <option key={item.name} value={item.name}>{item.name}</option>)}
          </select>
        </div>
        {catalogError && <InlineError title="Couldn’t load catalogs" message="Check the workspace connection and catalog permissions." onRetry={() => setCatalogAttempt((value) => value + 1)} />}
        {!catalog && !catalogError && (
          <div className="source-guide"><Icon name="folder" /><strong>Start with a catalog</strong><p>Choose a catalog, then select the schemas you’d like to see together on the canvas.</p></div>
        )}
        {catalog && (
          <section aria-label="Schemas">
            <div className="section-label">
              <span>Schemas {picked.size > 0 && <span className="badge">{picked.size} selected</span>}</span>
              {picked.size > 0 && <button className="text-button" disabled={loadingModel} onClick={() => setPicked(new Set())}>Clear</button>}
            </div>
            <div className="search">
              <Icon name="search" className="icon-sm" />
              <input className="input" aria-label="Filter schemas" placeholder="Find a schema…" value={schemaQuery} onChange={(event) => setSchemaQuery(event.target.value)} />
            </div>
            {loadingSchemas ? <div className="loading-line"><Spinner /> Loading schemas…</div>
              : schemaError ? <InlineError title="Couldn’t load schemas" message={schemaError} onRetry={() => setSchemaAttempt((value) => value + 1)} />
                : <div className="list schema-list" role="group" aria-label="Select schemas">
                  {filteredSchemas.map((schema) => <button key={schema.name} className="list-row" role="checkbox" aria-checked={picked.has(schema.name)} onClick={() => toggle(schema.name)} disabled={loadingModel}>
                    <span className="chk">{picked.has(schema.name) && <Icon name="check" />}</span>
                    <Icon name="folder" className="icon-sm row-icon" /><span className="row-title" title={schema.name}>{schema.name}</span>
                  </button>)}
                  {!filteredSchemas.length && <p className="field-hint">{schemaQuery ? 'No schemas match. Try a different search.' : 'No schemas are available in this catalog.'}</p>}
                </div>}
          </section>
        )}
        <section className="section-divider">
          <button className="disclosure-button" onClick={() => setShowPipes((value) => !value)} aria-expanded={showPipes} aria-controls="pipeline-sources">
            <Icon name={showPipes ? 'chevronDown' : 'chevronRight'} /><span>Lakeflow pipelines</span><Icon name="gitBranch" className="icon-sm" />
          </button>
          <p className="disclosure-hint">Add lineage and inferred relationships</p>
          {showPipes && <div id="pipeline-sources" className="disclosure-content">
            {!entities?.length && <p className="field-hint">Load a diagram first to attach pipeline lineage.</p>}
            <div className="search"><Icon name="search" className="icon-sm" /><input className="input" aria-label="Filter pipelines" placeholder="Find a pipeline…" value={pipeQuery} onChange={(event) => setPipeQuery(event.target.value)} /></div>
            {pipeError ? <InlineError title="Couldn’t load pipelines" message={pipeError} onRetry={() => setPipeAttempt((value) => value + 1)} />
              : !pipes ? <div className="loading-line"><Spinner /> Loading pipelines…</div>
                : <div className="list" style={{ maxHeight: 220, overflowY: 'auto' }}>
                  {matchingPipes.slice(0, 60).map((pipe) => <button key={pipe.pipeline_id} className={`list-row ${attachedIds?.has(pipe.pipeline_id) ? 'is-attached' : ''}`}
                    onClick={() => attach(pipe)} disabled={attaching === pipe.pipeline_id || attachedIds?.has(pipe.pipeline_id) || !entities?.length}>
                    <span className="row-icon">{attaching === pipe.pipeline_id ? <Spinner /> : <Icon name="gitBranch" className="icon-sm" />}</span>
                    <span className="row-title" title={pipe.name}>{pipe.name}</span>
                    {attachedIds?.has(pipe.pipeline_id) && <Icon name="check" className="icon-sm" style={{ color: 'var(--db-success)' }} />}
                  </button>)}
                  {!matchingPipes.length && <p className="field-hint">{pipes.length ? 'No matching pipelines. Try a different search.' : <>No pipelines are visible to the app. Grant pipeline view access{appPrincipal ? <span title={appPrincipal}> to the app’s service principal</span> : null}, or paste the source below.</>}</p>}
                  {matchingPipes.length > 60 && <p className="field-hint">Showing 60 of {matchingPipes.length} pipelines. Search to narrow the list.</p>}
                </div>}
            <details className="paste-details">
              <summary>Paste pipeline source instead</summary>
              <div className="field"><label htmlFor="pipeline-name">Pipeline name <span className="muted">(optional)</span></label><input id="pipeline-name" className="input" placeholder="My pipeline" value={pasteName} onChange={(event) => setPasteName(event.target.value)} /></div>
              <div className="field"><label htmlFor="pipeline-source">Python or SQL source</label><textarea id="pipeline-source" className="textarea mono" rows={6} placeholder="Paste your Lakeflow pipeline here…" value={pasteSrc} onChange={(event) => setPasteSrc(event.target.value)} /></div>
              <button className="btn btn-sm btn-block" disabled={!pasteSrc.trim() || parsing || !entities?.length} onClick={attachPasted}>
                {parsing ? <><Spinner /> Reading source…</> : <><Icon name="gitBranch" className="icon-sm" /> Add pipeline lineage</>}
              </button>
            </details>
          </div>}
        </section>
      </div>
      <div className="panel-foot">
        <p className="panel-foot-note">{picked.size ? `${picked.size} schema${picked.size === 1 ? '' : 's'} selected` : 'Select one or more schemas to begin'}</p>
        <button className="btn btn-primary btn-block" disabled={!picked.size || loadingModel} onClick={() => onConfirmReplace('Load diagram', loadDiagram)}>
          {loadingModel ? <><Spinner /> Building your diagram…</> : <><Icon name="database" className="icon-sm" /> Load diagram</>}
        </button>
      </div>
    </>
  );
}
