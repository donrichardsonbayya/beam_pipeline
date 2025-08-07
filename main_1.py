import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
import psycopg2
from datetime import datetime
import time
from apache_beam.transforms.window import FixedWindows

class ParseJSON(beam.DoFn):
    def process(self, element):
        try:
            record = json.loads(element)
            yield record
        except Exception as e:
            yield beam.pvalue.TaggedOutput('malformed', element)

class EnrichAndFormat(beam.DoFn):
    def process(self, record):
        try:
            record['event_time'] = datetime.strptime(record['timestamp'], "%Y-%m-%dT%H:%M:%S")
            yield record
        except Exception as e:
            print("Enrichment failed:", e)

class WriteToPostgres(beam.DoFn):
    def setup(self):
        self.conn = psycopg2.connect(
            dbname="beam_test", user="postgres", password="password",
            host="localhost", port="5432"
        )
        self.cursor = self.conn.cursor()

    def process(self, record):
        try:
            query = """
            INSERT INTO orders_stream (
                order_id, user_id, product_id, product_name,
                price, quantity, location, event_time
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO NOTHING
            """
            self.cursor.execute(query, (
                record['order_id'], record['user_id'], record['product_id'],
                record['product_name'], record['price'], record['quantity'],
                record['location'], record['event_time']
            ))
            self.conn.commit()
        except Exception as e:
            print("DB insert failed:", e)

    def teardown(self):
        self.cursor.close()
        self.conn.close()

class AttachWindowInfo(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        product_name, (total_revenue, order_count) = element
        yield (product_name, {
            'total_revenue': total_revenue,
            'order_count': order_count
        })

class WriteAggregatedMetrics(beam.DoFn):
    def setup(self):
        self.conn = psycopg2.connect(
            dbname="beam_test", user="postgres", password="password",
            host="localhost", port="5432"
        )
        self.cursor = self.conn.cursor()

    def process(self, element, window=beam.DoFn.WindowParam):
        product_name, values = element
        total_revenue = values['total_revenue']
        order_count = values['order_count']

        query = """
        INSERT INTO product_metrics (
            product_name, window_start, window_end,
            total_revenue, order_count
        ) VALUES (%s, %s, %s, %s, %s)
        """
        self.cursor.execute(query, (
            product_name,
            window.start.to_utc_datetime(),
            window.end.to_utc_datetime(),
            total_revenue,
            order_count
        ))
        self.conn.commit()

    def teardown(self):
        self.cursor.close()
        self.conn.close()

def run():
    options = PipelineOptions()
    with beam.Pipeline(options=options) as p:
        enriched = (
            p
            | 'ReadFromFile' >> beam.io.ReadFromText('stream_buffer.json')
            | 'ParseJSON' >> beam.ParDo(ParseJSON())
            | 'Enrich' >> beam.ParDo(EnrichAndFormat())
        )

        # 1. Write raw data to orders_stream
        enriched | 'WriteRawOrders' >> beam.ParDo(WriteToPostgres())

        # 2. Windowed aggregation and write to product_metrics
        (
            enriched
            | 'AddEventTime' >> beam.Map(lambda record: beam.window.TimestampedValue(record, record['event_time'].timestamp()))
            | 'WindowInto1Min' >> beam.WindowInto(FixedWindows(60), allowed_lateness=30)
            | 'KeyByProduct' >> beam.Map(lambda record: (record['product_name'], record))
            | 'MapToTuple' >> beam.Map(lambda r: (r[0], (r[1]['price'] * r[1]['quantity'], 1)))
            | 'SumRevenueAndCount' >> beam.CombinePerKey(beam.combiners.TupleCombineFn(sum, sum))
            | 'AttachWindow' >> beam.ParDo(AttachWindowInfo())
            | 'WriteMetrics' >> beam.ParDo(WriteAggregatedMetrics())
        )

    # Optional cleanup
    with open('stream_buffer.json', 'w') as f:
        f.truncate()

if __name__ == '__main__':
    while True:
        run()
        print("✔ Pipeline run complete. Waiting 10s...\n")
        time.sleep(10)
