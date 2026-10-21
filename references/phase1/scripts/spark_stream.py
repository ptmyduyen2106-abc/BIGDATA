import argparse
import json
import os
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, types as T

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--run-id',required=True)
    args=p.parse_args()
    out=Path('/app/outputs')/args.run_id
    expected=json.loads((out/'producer.json').read_text())
    spark=(SparkSession.builder.appName('RetailrocketEnvironmentCheck').master('local[2]')
           .config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions','2').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    schema=T.StructType([T.StructField(n,t) for n,t in [
        ('run_id',T.StringType()),('event_id',T.StringType()),('timestamp',T.LongType()),
        ('visitorid',T.LongType()),('event',T.StringType()),('itemid',T.LongType()),
        ('transactionid',T.LongType()),('ingested_at',T.StringType())]])
    raw=(spark.readStream.format('kafka').option('kafka.bootstrap.servers',os.getenv('KAFKA_BOOTSTRAP_SERVERS','kafka:29092'))
         .option('subscribe','retailrocket.events').option('startingOffsets','earliest').load())
    events=(raw.select(F.from_json(F.col('value').cast('string'),schema).alias('e'),'partition','offset')
            .select('e.*','partition','offset').filter(F.col('run_id')==args.run_id)
            .withColumn('event_time',F.expr('timestamp_millis(timestamp)')))
    query=(events.writeStream.format('parquet').option('path',str(out/'parquet'))
           .option('checkpointLocation',str(out/'checkpoint')).trigger(availableNow=True).start())
    query.awaitTermination()
    result=spark.read.parquet(str(out/'parquet'))
    received=result.count()
    unique=result.select('event_id').distinct().count()
    invalid=result.filter(F.col('event_id').isNull()|F.col('timestamp').isNull()|F.col('visitorid').isNull()|
        F.col('itemid').isNull()|F.col('event').isNull()|(~F.col('event').isin('view','addtocart','transaction'))).count()
    counts={r['event']:r['count'] for r in result.groupBy('event').count().collect()}
    summary=dict(run_id=args.run_id,spark_version=spark.version,received=received,unique_event_ids=unique,
                 invalid_rows=invalid,event_counts=counts,expected=expected['sent'],
                 passed=received==unique==expected['sent'] and invalid==0 and counts==expected['event_counts'])
    (out/'verification.json').write_text(json.dumps(summary,indent=2))
    result.orderBy('event_time').show(10,truncate=False)
    print('VERIFICATION '+json.dumps(summary))
    spark.stop()
    if not summary['passed']: raise RuntimeError('End-to-end verification failed')

if __name__=='__main__': main()
