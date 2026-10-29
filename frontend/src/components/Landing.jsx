import React from 'react';
import { Icon } from '../lib/icons';
import { createSampleModel } from '../lib/sampleModel';

const sample = createSampleModel();
const CAPABILITIES = [
  { icon: 'gitBranch', title: 'The whole picture', text: 'Tables, keys, relationships, and pipeline lineage. Connected.' },
  { icon: 'sparkles', title: 'A little help from AI', text: 'Find missing keys, refine a design, or build a model from raw files.' },
  { icon: 'code', title: 'From canvas to code', text: 'Generate SQL, migrations, and Lakeflow pipelines from your model.' },
];

function ModelPreview({ onSample }) {
  return (
    <div className="preview-window">
      <div className="preview-topbar">
        <Icon name="model" /><span className="preview-filename">commerce_model</span>
        <span className="preview-sample">Sample model</span>
      </div>
      <div className="preview-diagram" role="img" aria-label="A sample commerce model connecting customers, orders, products, and order items.">
        <svg className="preview-connections" viewBox="0 0 560 414" preserveAspectRatio="none" aria-hidden="true">
          <path d="M252 89 H270 Q280 89 280 99 V144 Q280 154 290 154 H308" />
          <path d="M420 195 V274" />
          <path d="M252 295 H270 Q280 295 280 305 V329 Q280 339 290 339 H308" />
          <circle cx="252" cy="89" r="3" /><circle cx="308" cy="154" r="3" />
          <circle cx="420" cy="195" r="3" /><circle cx="420" cy="274" r="3" />
          <circle cx="252" cy="295" r="3" /><circle cx="308" cy="339" r="3" />
          <text x="262" y="79">1</text><text x="291" y="145">∞</text>
          <text x="429" y="217">1</text><text x="429" y="261">∞</text>
          <text x="262" y="285">1</text><text x="291" y="330">∞</text>
        </svg>
        <span className="preview-model-label"><Icon name="link" /> 3 relationships</span>
        {sample.entities.map((entity) => (
          <div key={entity.id} className={`preview-node ${entity.id}`} style={{ '--layer': `var(--layer-${entity.layer})` }}>
            <div className="preview-node-header"><Icon name="table" /><span>{entity.name}</span><small>{entity.layer === 'dimension' ? 'dimension' : 'fact'}</small></div>
            {entity.columns.slice(0, 3).map((column) => (
              <div key={column.name} className="preview-column">
                {column.is_primary_key ? <Icon name="key" /> : column.is_foreign_key ? <Icon name="link" className="foreign-key" /> : <span />}
                <span className="column-name">{column.name}</span>
                <span className="column-type">{column.type.toLowerCase()}</span>
              </div>
            ))}
          </div>
        ))}
      </div>
      <button className="sample-launch" onClick={onSample}>
        <Icon name="play" className="icon-sm" /><span>Take a look around. <strong>Open the sample model</strong></span><Icon name="arrowUpRight" className="icon-sm" />
      </button>
    </div>
  );
}

export default function Landing({ onMode, onSample, hasModel, onResume }) {
  return (
    <main className="landing fade-in">
      <div className="landing-inner">
        <div className="landing-hero">
          <section className="landing-intro" aria-labelledby="welcome-title">
            <span className="intro-kicker"><Icon name="layers" /> Your lakehouse, connected</span>
            <h1 id="welcome-title">See the shape<br />{' '}of your data.</h1>
            <p className="lede">Explore existing schemas. Design what’s next. Bring your lakehouse into one clear view.</p>
            <div className="entry-grid">
              <button className="entry-card feature" onClick={() => onMode('explore')}>
                <span className="ec-badge"><Icon name="database" /></span>
                <span className="entry-copy"><strong>Explore Unity Catalog</strong><span>Turn existing schemas into an interactive model.</span></span>
                <span className="ec-go"><Icon name="arrowRight" /></span>
              </button>
              <button className="entry-card" onClick={() => onMode('design')}>
                <span className="ec-badge"><Icon name="model" /></span>
                <span className="entry-copy"><strong>Design a new model</strong><span>Start from a blank canvas or build from raw files.</span></span>
                <span className="ec-go"><Icon name="arrowRight" /></span>
              </button>
            </div>
            {hasModel
              ? <button className="text-button landing-note" onClick={onResume}><Icon name="arrowRight" className="icon-sm" /> Return to your current model</button>
              : <p className="landing-note"><Icon name="sparkles" /> Your ideas, with an AI modeling partner alongside.</p>}
          </section>
          <ModelPreview onSample={onSample} />
        </div>
        <div className="capability-strip">
          {CAPABILITIES.map((item) => <div key={item.title} className="capability"><Icon name={item.icon} /><div><strong>{item.title}</strong><p>{item.text}</p></div></div>)}
        </div>
        <footer className="landing-footer">
          <span><Icon name="layers" className="icon-sm" /> Built on Databricks</span><span>A clear path from first sketch to production.</span>
        </footer>
      </div>
    </main>
  );
}
