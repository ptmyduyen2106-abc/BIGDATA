"""Download original Retailrocket files from Kaggle, retaining provenance and hashes."""
import argparse
import csv
import hashlib
import json
import shutil
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FILES = {
    'events.csv': ['timestamp', 'visitorid', 'event', 'itemid', 'transactionid'],
    'category_tree.csv': ['categoryid', 'parentid'],
    'item_properties_part1.csv': ['timestamp', 'itemid', 'property', 'value'],
    'item_properties_part2.csv': ['timestamp', 'itemid', 'property', 'value'],
}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--all', action='store_true', help='Also download both large item-property files')
    args = parser.parse_args()
    raw, downloads = ROOT / 'data/raw', ROOT / 'data/downloads'
    raw.mkdir(parents=True, exist_ok=True)
    downloads.mkdir(parents=True, exist_ok=True)
    manifest_path = raw / 'manifest.json'
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    for name in (list(FILES) if args.all else ['events.csv', 'category_tree.csv']):
        target = raw / name
        url = f'https://www.kaggle.com/api/v1/datasets/download/retailrocket/ecommerce-dataset/{name}'
        if not target.exists():
            temp = downloads / (name + '.download')
            print(f'Downloading {name}', flush=True)
            with urllib.request.urlopen(url, timeout=120) as response, temp.open('wb') as output:
                shutil.copyfileobj(response, output)
            if zipfile.is_zipfile(temp):
                with zipfile.ZipFile(temp) as archive:
                    matches = [n for n in archive.namelist() if Path(n).name == name]
                    if len(matches) != 1:
                        raise ValueError(f'Expected one {name} in download')
                    with archive.open(matches[0]) as source, target.open('wb') as output:
                        shutil.copyfileobj(source, output)
            else:
                shutil.copyfile(temp, target)
        with target.open(encoding='utf-8-sig', newline='') as source:
            if next(csv.reader(source)) != FILES[name]:
                raise ValueError(f'Unexpected CSV header: {target}')
        with target.open('rb') as source:
            digest = hashlib.file_digest(source, 'sha256').hexdigest()
        manifest[name] = dict(source_url=url, source_page='https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset',
                              bytes=target.stat().st_size, sha256=digest,
                              verified_at_utc=datetime.now(timezone.utc).isoformat())
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding='utf-8')
        print(f'{name}: {target.stat().st_size:,} bytes; SHA256 {digest}', flush=True)

if __name__ == '__main__':
    main()
