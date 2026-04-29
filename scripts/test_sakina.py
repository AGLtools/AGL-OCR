"""Quick test for SakinaExtractor."""
import sys
sys.path.insert(0, r"c:\AGL_OCR")
from pathlib import Path
from src.manifest_parser import ManifestParser

pdfs = list(Path(r"c:\AGL_OCR\testing_pdf").glob("*.pdf"))
if not pdfs:
    print("ERROR: no PDF in testing_pdf/")
    sys.exit(1)

pdf = pdfs[0]
print(f"PDF: {pdf.name}")

parser = ManifestParser()
rows = parser.parse_scanned(pdf)
print(f"Lignes extraites: {len(rows)}")

fields = ["vessel","voyage","date_of_arrival","port_of_loading","port_of_discharge",
          "bl_number","shipper","consignee","description","weight","_transit_to","_shipowner"]

for i, r in enumerate(rows):
    print(f"\n--- Ligne {i+1} ---")
    for k in fields:
        print(f"  {k:<25} = {r.get(k,'')}")
