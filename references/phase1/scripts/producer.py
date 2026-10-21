import argparse
import csv
import json
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from kafka import KafkaProducer

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--run-id', required=True)
    p.add_argument('--limit',type=int,default=1000)
    p.add_argument('--input',default='/app/data/sample/events_10000.csv')
    args = p.parse_args()
    if args.limit <= 0:
        p.error('--limit must be positive')
    producer = KafkaProducer(bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS','kafka:29092'),
        acks='all',enable_idempotence=True,
        key_serializer=lambda x:str(x).encode(),value_serializer=lambda x:json.dumps(x).encode())
    pending, counts = [], Counter()
    with open(args.input,newline='',encoding='utf-8') as f:
        for row_id,row in enumerate(csv.DictReader(f),1):
            if row_id > args.limit: break
            event = dict(run_id=args.run_id,event_id=f'{args.run_id}:{row_id}',
                         timestamp=int(row['timestamp']),visitorid=int(row['visitorid']),
                         event=row['event'],itemid=int(row['itemid']),
                         transactionid=int(row['transactionid']) if row['transactionid'] else None,
                         ingested_at=datetime.now(timezone.utc).isoformat())
            pending.append(producer.send('retailrocket.events',key=event['visitorid'],value=event))
            counts[event['event']] += 1
    for result in pending: result.get(timeout=60)
    producer.close()
    if not pending: raise RuntimeError('No input events')
    result=dict(run_id=args.run_id,sent=len(pending),event_counts=dict(counts),input_file=args.input)
    out=Path('/app/outputs') / args.run_id
    out.mkdir(parents=True,exist_ok=True)
    (out/'producer.json').write_text(json.dumps(result,indent=2))
    print(json.dumps(result,indent=2))

if __name__=='__main__': main()
