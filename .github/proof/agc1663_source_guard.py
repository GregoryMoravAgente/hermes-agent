from pathlib import Path
import hashlib,json
root=Path(__file__).resolve().parents[2]
manifest=json.loads((root/'.github/proof/agc1663-source-selection.json').read_text())
for rel,expected in manifest['sourceFiles'].items():
    if hashlib.sha256((root/rel).read_bytes()).hexdigest()!=expected:
        raise SystemExit('frozen source mismatch: '+rel)
print('All declared source hashes match the frozen composition.')
