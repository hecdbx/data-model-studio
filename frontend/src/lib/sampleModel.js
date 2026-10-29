// An editable, local example. It never queries or creates workspace resources.
export function createSampleModel() {
  const column = (name, type, extra = {}) => ({ name, type, nullable: true, ...extra });
  const key = (name) => column(name, 'BIGINT', { nullable: false, is_primary_key: true });
  const foreignKey = (name, entity, target) => column(name, 'BIGINT', {
    nullable: false, is_foreign_key: true, references: { entity, column: target },
  });
  return {
    name: 'commerce_model',
    model_type: 'physical',
    description: 'A sample commerce model connecting customers, orders, products, and order items. Select any table to make it your own.',
    source: { kind: 'sample' },
    entities: [
      { id: 'customers', name: 'customers', layer: 'dimension', schema: 'commerce',
        description: 'Customer profiles and contact details.', primary_key: ['customer_id'],
        position: { x: 0, y: 0 },
        columns: [key('customer_id'), column('name', 'STRING'), column('email', 'STRING'), column('created_at', 'TIMESTAMP')] },
      { id: 'orders', name: 'orders', layer: 'fact', schema: 'commerce',
        description: 'Orders placed by customers.', primary_key: ['order_id'],
        position: { x: 440, y: 0 },
        columns: [key('order_id'), foreignKey('customer_id', 'customers', 'customer_id'), column('ordered_at', 'TIMESTAMP'), column('total_amount', 'DECIMAL(12,2)')] },
      { id: 'products', name: 'products', layer: 'dimension', schema: 'commerce',
        description: 'Products available to order.', primary_key: ['product_id'],
        position: { x: 0, y: 310 },
        columns: [key('product_id'), column('product_name', 'STRING'), column('category', 'STRING'), column('price', 'DECIMAL(12,2)')] },
      { id: 'order_items', name: 'order_items', layer: 'fact', schema: 'commerce',
        description: 'Individual products and quantities within each order.', primary_key: ['item_id'],
        position: { x: 440, y: 310 },
        columns: [key('item_id'), foreignKey('order_id', 'orders', 'order_id'), foreignKey('product_id', 'products', 'product_id'), column('quantity', 'INT')] },
    ],
    relationships: [
      { id: 'orders_customers', from_entity: 'orders', from_columns: ['customer_id'], to_entity: 'customers', to_columns: ['customer_id'], type: 'many-to-one' },
      { id: 'items_orders', from_entity: 'order_items', from_columns: ['order_id'], to_entity: 'orders', to_columns: ['order_id'], type: 'many-to-one' },
      { id: 'items_products', from_entity: 'order_items', from_columns: ['product_id'], to_entity: 'products', to_columns: ['product_id'], type: 'many-to-one' },
    ],
    pipelines: [],
    lineage: [],
  };
}
