import React, { useState } from 'react';
import { Icon } from '../lib/icons';
import {
  LAYERS, findEntity, addColumn, setColumns, updateEntity,
  removeEntity, removeRelationship, updateRelationship, stats,
} from '../lib/model';
import { Empty, Modal } from './common';

function ColumnRow({ col, index, onChange, onRemove }) {
  const set = (patch) => onChange({ ...col, ...patch });
  const required = col.is_primary_key || col.nullable === false;
  return (
    <div className="col-row">
      <div className="column-fields">
        <input className="input" aria-label={`Column ${index + 1} name`} value={col.name} spellCheck={false} onChange={(event) => set({ name: event.target.value })} placeholder="column_name" />
        <input className="input mono" aria-label={`Column ${index + 1} type`} value={col.type} spellCheck={false} onChange={(event) => set({ type: event.target.value })} placeholder="STRING" />
      </div>
      <div className="column-options">
        <span className="column-index">Column {index + 1}</span>
        <div className="col-flags">
          <button className="flag-btn pk" aria-pressed={!!col.is_primary_key} aria-label={`Primary key for ${col.name}`} title="Primary key"
            onClick={() => set({ is_primary_key: !col.is_primary_key, nullable: col.is_primary_key ? col.nullable : false })}><Icon name="key" style={{ width: 10, height: 10 }} /> PK</button>
          <button className="flag-btn fk" aria-pressed={!!col.is_foreign_key} aria-label={`Foreign key for ${col.name}`} title="Foreign key"
            onClick={() => set({ is_foreign_key: !col.is_foreign_key })}>FK</button>
          <button className="flag-btn nn" aria-pressed={!!required} aria-label={`Require a value for ${col.name}`} disabled={!!col.is_primary_key}
            title={col.is_primary_key ? 'Primary keys are always required' : 'Require a value (not null)'} onClick={() => set({ nullable: !col.nullable })}>Required</button>
        </div>
        <button className="flag-btn flag-remove" aria-label={`Remove column ${col.name}`} title="Remove column" onClick={onRemove}><Icon name="x" style={{ width: 11, height: 11 }} /></button>
      </div>
    </div>
  );
}

function EntityInspector({ model, entity, onModelChange, onSelect }) {
  const [deleteOpen, setDeleteOpen] = useState(false);
  const updateCol = (index, next) => onModelChange(setColumns(model, entity.id, entity.columns.map((column, i) => i === index ? next : column)));
  const removeCol = (index) => onModelChange(setColumns(model, entity.id, entity.columns.filter((_, i) => i !== index)));
  return (
    <div className="inspector fade-in">
      <div className="insp-title">
        <span className="entity-icon" style={{ '--layer': `var(--layer-${entity.layer || 'table'}, var(--layer-table))` }}><Icon name="table" /></span>
        <div><strong title={entity.name}>{entity.name}</strong><small>Table properties</small></div>
        {entity.is_new && <span className="badge">New</span>}
      </div>
      <div className="field">
        <label htmlFor="table-name">Table name</label>
        <input id="table-name" className="input" spellCheck={false} value={entity.name} onChange={(event) => onModelChange(updateEntity(model, entity.id, { name: event.target.value }))} />
      </div>
      <div className="field-pair">
        <div className="field">
          <label htmlFor="table-layer">Model layer</label>
          <select id="table-layer" className="select" value={entity.layer || 'table'} onChange={(event) => onModelChange(updateEntity(model, entity.id, { layer: event.target.value }))}>
            {LAYERS.map((layer) => <option key={layer} value={layer}>{layer.charAt(0).toUpperCase() + layer.slice(1)}</option>)}
          </select>
        </div>
        <div className="field">
          <label htmlFor="table-schema">Schema</label>
          <input id="table-schema" className="input" value={entity.schema || ''} placeholder="Target schema" spellCheck={false} onChange={(event) => onModelChange(updateEntity(model, entity.id, { schema: event.target.value || null }))} />
        </div>
      </div>
      <div className="field">
        <label htmlFor="table-description">Description</label>
        <textarea id="table-description" className="textarea" rows={3} placeholder="What does this table represent?" value={entity.description || ''} onChange={(event) => onModelChange(updateEntity(model, entity.id, { description: event.target.value }))} />
      </div>
      <section aria-label="Table columns">
        <div className="section-label"><span>Columns</span><span>{entity.columns.length}</span></div>
        <div className="col-editor">
          {entity.columns.map((column, i) => <ColumnRow key={i} col={column} index={i} onChange={(next) => updateCol(i, next)} onRemove={() => removeCol(i)} />)}
        </div>
        <button className="btn btn-sm btn-block add-column" onClick={() => onModelChange(addColumn(model, entity.id))}><Icon name="plus" className="icon-sm" /> Add column</button>
      </section>
      <div className="inspector-delete"><button className="btn btn-sm btn-ghost" style={{ color: 'var(--db-error)' }} onClick={() => setDeleteOpen(true)}><Icon name="trash" className="icon-sm" /> Delete table</button></div>
      {deleteOpen && <Modal title={`Delete “${entity.name}”?`} description="This removes the table and its relationships from the canvas." icon="trash" onClose={() => setDeleteOpen(false)}>
        <div className="modal-actions"><button className="btn" onClick={() => setDeleteOpen(false)}>Keep table</button><button className="btn btn-danger" onClick={() => { onModelChange(removeEntity(model, entity.id)); onSelect(null); }}>Delete table</button></div>
      </Modal>}
    </div>
  );
}

function RelationshipInspector({ model, rel, onModelChange, onSelect }) {
  const from = findEntity(model, rel.from_entity);
  const to = findEntity(model, rel.to_entity);
  const setCols = (key, value) => onModelChange(updateRelationship(model, rel.id, { [key]: value.split(',').map((item) => item.trim()).filter(Boolean) }));
  return (
    <div className="inspector fade-in">
      <div className="insp-title"><span className="entity-icon" style={{ '--layer': 'var(--selection)' }}><Icon name="gitBranch" /></span><div><strong>Relationship</strong><small>Connect your data, deliberately</small></div></div>
      <div className="relationship-path"><span title={from?.name || rel.from_entity}>{from?.name || rel.from_entity}</span><Icon name="arrowRight" /><span title={to?.name || rel.to_entity}>{to?.name || rel.to_entity}</span></div>
      <div className="field">
        <label htmlFor="foreign-key-columns">Foreign key columns in {from?.name || 'child table'}</label>
        <input id="foreign-key-columns" className="input mono" spellCheck={false} value={(rel.from_columns || []).join(', ')} onChange={(event) => setCols('from_columns', event.target.value)} placeholder="customer_id" />
      </div>
      <div className="field">
        <label htmlFor="referenced-columns">Referenced columns in {to?.name || 'parent table'}</label>
        <input id="referenced-columns" className="input mono" spellCheck={false} value={(rel.to_columns || []).join(', ')} onChange={(event) => setCols('to_columns', event.target.value)} placeholder="id" />
        <p className="field-hint">Separate multiple columns with commas.</p>
      </div>
      <div className="field">
        <label htmlFor="relationship-type">Cardinality</label>
        <select id="relationship-type" className="select" value={rel.type} onChange={(event) => onModelChange(updateRelationship(model, rel.id, { type: event.target.value }))}>
          <option value="many-to-one">Many to one (∞ : 1)</option><option value="one-to-many">One to many (1 : ∞)</option><option value="one-to-one">One to one (1 : 1)</option><option value="many-to-many">Many to many (∞ : ∞)</option>
        </select>
      </div>
      <div className="field"><label htmlFor="relationship-description">Description</label><textarea id="relationship-description" className="textarea" rows={3} value={rel.description || ''} placeholder="Describe how these tables relate…" onChange={(event) => onModelChange(updateRelationship(model, rel.id, { description: event.target.value }))} /></div>
      <div className="inspector-delete"><button className="btn btn-sm btn-ghost" style={{ color: 'var(--db-error)' }} onClick={() => { onModelChange(removeRelationship(model, rel.id)); onSelect(null); }}><Icon name="trash" className="icon-sm" /> Delete relationship</button></div>
    </div>
  );
}

function Overview({ model, onSelect }) {
  const counts = stats(model);
  return (
    <div className="inspector fade-in">
      <div className="inspector-intro"><h2>Model overview</h2><p>{model.description || 'A little structure goes a long way. Select a table or relationship to see its details here.'}</p></div>
      <div className="model-stats" aria-label="Model summary">
        <div className="model-stat"><strong>{counts.entities}</strong><span>Tables</span></div>
        <div className="model-stat"><strong>{counts.columns}</strong><span>Columns</span></div>
        <div className="model-stat"><strong>{counts.relationships}</strong><span>Relationships</span></div>
      </div>
      {model.entities.length ? <>
        <section aria-label="Model tables">
          <div className="section-label"><span>In this model</span><span>{model.entities.length} tables</span></div>
          <div className="overview-tables">{model.entities.map((entity) => <button key={entity.id} className="list-row overview-table" style={{ '--layer': `var(--layer-${entity.layer || 'table'}, var(--layer-table))` }} onClick={() => onSelect({ type: 'entity', id: entity.id, focus: true })}>
            <Icon name="table" /><span className="row-title" title={entity.name}>{entity.name}</span><span className="row-meta">{entity.columns.length} {entity.columns.length === 1 ? 'col' : 'cols'}</span><Icon name="chevronRight" />
          </button>)}</div>
        </section>
        <div className="overview-tip"><Icon name="eye" /><p>Select a table to edit its columns and keys, or select a connection to refine a relationship.</p></div>
      </> : <Empty icon="table" title="Ready for your first table">Add a table on the canvas or load a schema to see your model here.</Empty>}
    </div>
  );
}

export default function Inspector({ model, selection, onModelChange, onSelect }) {
  if (selection?.type === 'entity') {
    const entity = model.entities.find((item) => item.id === selection.id);
    if (entity) return <EntityInspector key={entity.id} model={model} entity={entity} onModelChange={onModelChange} onSelect={onSelect} />;
  }
  if (selection?.type === 'relationship') {
    const relationship = model.relationships.find((item) => item.id === selection.id);
    if (relationship) return <RelationshipInspector model={model} rel={relationship} onModelChange={onModelChange} onSelect={onSelect} />;
  }
  return <Overview model={model} onSelect={onSelect} />;
}
