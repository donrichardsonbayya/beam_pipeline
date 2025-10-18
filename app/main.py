import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
import json, os, psycopg2, time
from datetime import datetime

# --- DB config from env (Compose) ---
PG_HOST = os.getenv("PGHOST", "localhost")
PG_DB   = os.getenv("PGDATABASE", "beam_test")
PG_USER = os.getenv("PGUSER", "postgres")
PG_PW   = os.getenv("PGPASSWORD", "password")
PG_PORT = int(os.getenv("PGPORT", "5432"))

BUFFER_FILE = "stream_buffer.json"

# ---------- DoFns ----------
class ParseJSON(beam.DoFn):
    def process(self, element):
        try:
            yield json.loads(element)
        except Exception:
            # silently drop malformed lines; keep pipeline flowing
            return []

class EnrichAndFormat(beam.DoFn):
    def process(self, record):
        try:
            # strict parse: 2025-10-14T12:34:56  (no Z)
            record["event_time"] = datetime.strptime(record["timestamp"].rstrip("Z"), "%Y-%m-%dT%H:%M:%S")
            # coerce numeric types if they arrived as strings
            record["quantity"] = int(record["quantity"])
            record["price"] = float(record["price"])
            yield record
        except Exception as e:
            # bad row -> drop (prevents DB exceptions)
            print("Enrichment dropped row:", e, "payload=", record)
            return []

class WriteToPostgres(beam.DoFn):
    def setup(self):
        self.conn = psycopg2.connect(
            host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PW, port=PG_PORT
        )
        self.conn.autocommit = True
        self.cur = self.conn.cursor()

    def process(self, record):
        self.cur.execute(
            """
            INSERT INTO orders_stream(
              order_id, user_id, product_id, product_name,
              price, quantity, location, event_time
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (order_id) DO NOTHING
            """,
            (
                record["order_id"], record["user_id"], record["product_id"],
                record["product_name"], record["price"], record["quantity"],
                record["location"], record["event_time"]
            ),
        )
        # Beam side-effect DoFn -> return empty iterable
        return []

    def teardown(self):
        try: self.cur.close()
        finally: self.conn.close()

class WriteAggregatedMetrics(beam.DoFn):
    def setup(self):
        self.conn = psycopg2.connect(
            host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PW, port=PG_PORT
        )
        self.conn.autocommit = True
        self.cur = self.conn.cursor()

    def process(self, kv, window=beam.DoFn.WindowParam):
        product, (total_revenue, order_count) = kv
        self.cur.execute(
            """
            INSERT INTO product_metrics(
              product_name, window_start, window_end, total_revenue, order_count
            )
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (product_name, window_start) DO UPDATE
              SET window_end = EXCLUDED.window_end,
                  total_revenue = EXCLUDED.total_revenue,
                  order_count = EXCLUDED.order_count
            """,
            (
                product,
                window.start.to_utc_datetime(),
                window.end.to_utc_datetime(),
                total_revenue,
                order_count,
            ),
        )
        return []

    def teardown(self):
        try: self.cur.close()
        finally: self.conn.close()

# ---------- Pipeline ----------
def run_once():
    opts = PipelineOptions()
    with beam.Pipeline(options=opts) as p:
        enriched = (
            p
            | "ReadFromFile" >> beam.io.ReadFromText(BUFFER_FILE)
            | "ParseJSON" >> beam.ParDo(ParseJSON())
            | "Enrich" >> beam.ParDo(EnrichAndFormat())
        )

        # Raw sink
        _ = enriched | "WriteRawOrders" >> beam.ParDo(WriteToPostgres())

        # Windowed metrics: 1-min windows, 30s lateness
        (
            enriched
            | "AddEventTime" >> beam.Map(lambda r: beam.window.TimestampedValue(r, r["event_time"].timestamp()))
            | "WindowInto1Min" >> beam.WindowInto(FixedWindows(60), allowed_lateness=30)
            | "KeyByProduct" >> beam.Map(lambda r: (r["product_name"], (r["price"] * r["quantity"], 1)))
            | "SumRevenueAndCount" >> beam.CombinePerKey(beam.combiners.TupleCombineFn(sum, sum))
            | "WriteMetrics" >> beam.ParDo(WriteAggregatedMetrics())
        )

    # truncate buffer so each run processes only new lines
    try:
        open(BUFFER_FILE, "w").close()
    except Exception:
        pass

if __name__ == "__main__":
    while True:
        run_once()
        print("✔ Pipeline run complete. Waiting 10s...\n")
        time.sleep(10)
