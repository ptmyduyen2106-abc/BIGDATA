"""Select a REAL future source event, then verify live seen-item cache filtering.

prepare writes one existing RetailRocket row, without modifying its fields.
Run pipeline.streaming and publish that CSV with the SAME run ID, then verify.
The API must still serve the chosen run (or explicitly pin RUN_ID while testing).
"""
import argparse
import csv
import json
from pathlib import Path
import time
import urllib.request

from pipeline.config import atomic_json, run_path, utc_now


def get(base, route):
    with urllib.request.urlopen(base.rstrip('/') + route, timeout=20) as response:
        return json.load(response)


def prepare(args):
    root = run_path(args.run_id, args.output_root)
    bundle = json.loads((root / 'serving/model_bundle.json').read_text(encoding='utf-8'))
    kpis = json.loads((root / 'metrics/kpis.json').read_text(encoding='utf-8'))
    from datetime import datetime
    latest = int(datetime.fromisoformat(kpis['last_event_time_utc'].replace('Z', '+00:00')).timestamp() * 1000)
    candidates = {int(user): {int(row['itemid']) for row in rows[:10]}
                  for user, rows in bundle['recommendations'].items()}
    with open(args.source, newline='', encoding='utf-8-sig') as source:
        reader = csv.DictReader(source)
        for line, row in enumerate(reader, start=2):
            user, item = int(row['visitorid']), int(row['itemid'])
            if int(row['timestamp']) <= latest or item not in candidates.get(user, set()):
                continue
            before = get(args.base_url, f'/api/recommendations/{user}?k=10')
            if before['run_id'] != args.run_id:
                raise RuntimeError('API active run changed; pin RUN_ID for an isolated live check')
            if item not in {value['itemid'] for value in before['items']}:
                continue
            target = Path(args.event_csv)
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open('w', newline='', encoding='utf-8') as out:
                writer = csv.DictWriter(out, fieldnames=reader.fieldnames)
                writer.writeheader()
                writer.writerow(row)
            report = {'status': 'prepared', 'run_id': args.run_id, 'source_csv': args.source,
                      'source_line_including_header': line, 'real_source_row': row,
                      'event_csv': args.event_csv, 'before': before, 'prepared_at_utc': utc_now()}
            atomic_json(args.report, report)
            return report
    raise RuntimeError('No future actual source event matches a current recommendation')


def verify(args):
    import os
    import redis
    from pymongo import MongoClient
    report = json.loads(Path(args.report).read_text(encoding='utf-8'))
    user, item = int(report['real_source_row']['visitorid']), int(report['real_source_row']['itemid'])
    cache = redis.Redis.from_url(os.getenv('REDIS_URL', 'redis://redis:6379/0'), decode_responses=True)
    deadline = time.monotonic() + args.timeout
    try:
        while True:
            seen = cache.sismember(f'bigdata:{args.run_id}:seen:{user}', str(item))
            response = get(args.base_url, f'/api/recommendations/{user}?k=10')
            if response['run_id'] != args.run_id:
                raise RuntimeError('API switched runs during check; pin RUN_ID to repeat')
            if seen and item not in {value['itemid'] for value in response['items']}:
                break
            if time.monotonic() >= deadline:
                raise TimeoutError('Live item was not observed and excluded within timeout')
            time.sleep(2)
        with MongoClient(os.getenv('MONGO_URI', 'mongodb://mongodb:27017')) as mongo:
            matches = mongo.bigdata.events.count_documents({'run_id': args.run_id,
                'visitorid': user, 'itemid': item, 'timestamp': int(report['real_source_row']['timestamp'])})
            # Stored field naming is inspected explicitly if a backend evolves.
            report['mongo_matching_facts'] = matches
            assert matches >= 1, 'Actual event not found in MongoDB fact collection'
        report.update(status='passed', after=response, redis_seen_item=True, verified_at_utc=utc_now())
        atomic_json(args.report, report)
        return report
    finally:
        cache.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['prepare', 'verify'])
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--output-root', default='outputs')
    parser.add_argument('--base-url', default='http://api:8000')
    parser.add_argument('--source', default='data/raw/events.csv')
    parser.add_argument('--event-csv', default='data/sample/live_check.csv')
    parser.add_argument('--report', default='outputs/evidence/live_check.json')
    parser.add_argument('--timeout', type=float, default=120)
    args = parser.parse_args()
    print(json.dumps(prepare(args) if args.command == 'prepare' else verify(args), indent=2))
