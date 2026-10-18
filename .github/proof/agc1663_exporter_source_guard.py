from pathlib import Path
import hashlib
import json
root = Path(__file__).resolve().parents[2]
manifest = json.loads((root / '.github/proof/agc1663-exporter-source-selection.json').read_text())
for relative, expected in manifest['sourceFiles'].items():
    if hashlib.sha256((root / relative).read_bytes()).hexdigest() != expected:
        raise SystemExit('frozen source mismatch: ' + relative)
print(f"All {len(manifest['sourceFiles'])} frozen exporter source/input hashes match.")
