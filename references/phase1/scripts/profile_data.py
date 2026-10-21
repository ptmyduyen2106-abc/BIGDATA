"""Exact full-file statistics using disk-backed SQLite, without loading all rows into RAM."""
import csv
import json
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def main():
    output = ROOT / 'outputs'
    sample = ROOT / 'data/sample'
    output.mkdir(exist_ok=True)
    sample.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='retailrocket-profile-') as temp:
        db = sqlite3.connect(str(Path(temp) / 'profile.db'))
        db.execute('CREATE TABLE events (source_row INTEGER, timestamp INTEGER, visitorid INTEGER, event TEXT, itemid INTEGER, transactionid TEXT)')
        missing = dict.fromkeys(['timestamp','visitorid','event','itemid','transactionid'],0)
        batch, invalid, total = [], 0, 0
        with (ROOT / 'data/raw/events.csv').open(newline='', encoding='utf-8') as f:
            for total, row in enumerate(csv.DictReader(f), 1):
                for field in missing:
                    missing[field] += int(not row[field].strip())
                try:
                    ts, visitor, item = int(row['timestamp']), int(row['visitorid']), int(row['itemid'])
                    transaction = row['transactionid']
                    if transaction:
                        int(transaction)
                    if ts <= 0 or visitor < 0 or item < 0 or row['event'] not in ('view','addtocart','transaction'):
                        raise ValueError('Invalid event')
                    batch.append((total,ts,visitor,row['event'],item,transaction))
                except (ValueError, TypeError):
                    invalid += 1
                if len(batch) >= 10000:
                    db.executemany('INSERT INTO events VALUES (?,?,?,?,?,?)',batch)
                    batch.clear()
        db.executemany('INSERT INTO events VALUES (?,?,?,?,?,?)',batch)
        db.commit()
        counts = dict(db.execute('SELECT event, COUNT(*) FROM events GROUP BY event'))
        minimum, maximum = db.execute('SELECT MIN(timestamp), MAX(timestamp) FROM events').fetchone()
        stat = dict(total_rows=total, valid_rows=sum(counts.values()), invalid_rows=invalid,
                    events=counts, missing_values=missing,
                    unique_visitors=db.execute('SELECT COUNT(DISTINCT visitorid) FROM events').fetchone()[0],
                    unique_items=db.execute('SELECT COUNT(DISTINCT itemid) FROM events').fetchone()[0],
                    unique_transactions=db.execute("SELECT COUNT(DISTINCT transactionid) FROM events WHERE transactionid <> ''").fetchone()[0],
                    start_utc=datetime.fromtimestamp(minimum/1000,timezone.utc).isoformat(),
                    end_utc=datetime.fromtimestamp(maximum/1000,timezone.utc).isoformat(),
                    exact_duplicate_rows=db.execute('SELECT COALESCE(SUM(n-1),0) FROM (SELECT COUNT(*) n FROM events GROUP BY timestamp, visitorid, event, itemid, transactionid HAVING COUNT(*)>1)').fetchone()[0],
                    transaction_events_without_id=db.execute("SELECT COUNT(*) FROM events WHERE event='transaction' AND transactionid=''").fetchone()[0])
        # Deterministic chronological sample of genuine observations. No synthetic events.
        with (sample / 'events_10000.csv').open('w',newline='',encoding='utf-8') as f:
            writer=csv.writer(f)
            writer.writerow(['timestamp','visitorid','event','itemid','transactionid'])
            writer.writerows(db.execute('SELECT timestamp,visitorid,event,itemid,transactionid FROM events ORDER BY timestamp,source_row LIMIT 10000'))
        db.close()
    (output / 'data_profile.json').write_text(json.dumps(stat,indent=2),encoding='utf-8')
    print(json.dumps(stat,indent=2))

if __name__ == '__main__':
    main()
