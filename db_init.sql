CREATE TABLE IF NOT EXISTS orders_stream (
  order_id TEXT PRIMARY KEY,
  user_id TEXT,
  product_id TEXT,
  product_name TEXT,
  price NUMERIC(10,2),
  quantity INT,
  location TEXT,
  event_time TIMESTAMP
);

CREATE TABLE IF NOT EXISTS product_metrics (
  product_name TEXT,
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  total_revenue NUMERIC(12,2),
  order_count INT,
  PRIMARY KEY (product_name, window_start)
);
