from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from flask import Flask, request, render_template_string, Response

try:
	import fitz
except Exception as e:
	fitz = None
	print("Warning: PyMuPDF (fitz) failed to import:", e)


app = Flask(__name__)

DEFAULT_BASE_DIR = r"P:\\2025 Run Data"
REN_FOLDERS = ["Ren A", "Ren B", "Ren C"]
PART_TYPES = ["ALIF", "LLIF", "ALIF-X", "TLIF-C", "TLIF-O", "TLIF-CA"]

FLOAT = r"[-+]?(?:\d+\.\d+|\d+)"
RE_AP = re.compile(r"AP\s*Depth\s*\"?B\"?\s*\(mm\)\s+(" + FLOAT + r")\s+(" + FLOAT + r")\s+(" + FLOAT + r")", re.I)
RE_ML = re.compile(r"ML\s*Width\s*\"?A\"?\s*\(mm\)\s+(" + FLOAT + r")\s+(" + FLOAT + r")\s+(" + FLOAT + r")", re.I)
RE_MAX_H = re.compile(r"Max\s*Cage\s*Height\s*\"?C\"?\s*\(mm\)\s+(" + FLOAT + r")\s+(" + FLOAT + r")\s+(" + FLOAT + r")", re.I)
RE_IMPLANT = re.compile(r"IMPLANT_NAME=([^\s]+)", re.I)
RE_LOT = re.compile(r"\b\d{6}\.[A-Z]{2}\.\??\d{2}\b", re.I)
RE_PO = re.compile(r"(PO\d+)", re.I)

PLAN_LABELS = {0: "Minus 01", 1: "Plan 02", 2: "Plus 03"}


@dataclass
class Thresholds:
	ap_depth_b_mm: Optional[float]
	ml_width_a_mm: Optional[float]
	max_cage_height_c_mm: Optional[float]


def iter_job_cmd_dirs(base_dir: Path, rens: Sequence[str]) -> Iterable[Path]:
	for ren in rens:
		ren_dir = base_dir / ren
		try:
			if not ren_dir.is_dir():
				continue
			for job_dir in ren_dir.iterdir():
				if not job_dir.is_dir():
					continue
				cmd_dir = job_dir / "Docs" / "CMD"
				if cmd_dir.is_dir():
					yield cmd_dir
		except Exception as exc:
			print(f"Error scanning ren folder {ren_dir}: {exc}")


def find_form19_pdfs(cmd_path: Path) -> List[Path]:
	pdfs: List[Path] = []
	try:
		for p in cmd_path.rglob("*.pdf"):
			name_lower = p.name.lower()
			if ("form-019" in name_lower) or ("form-19" in name_lower):
				pdfs.append(p)
	except Exception as exc:
		print(f"Error while searching PDFs in {cmd_path}: {exc}")
	return pdfs


def _parse_three_numbers(match: Optional[re.Match]) -> Optional[Tuple[float, float, float]]:
	if not match:
		return None
	try:
		return (float(match.group(1)), float(match.group(2)), float(match.group(3)))
	except Exception:
		return None


def _extract_po_from_path(pdf_path: Path, cmd_root: Path) -> str:
	try:
		for parent in [pdf_path.parent, *pdf_path.parents]:
			if parent == cmd_root:
				break
			m = RE_PO.search(parent.name)
			if m:
				return m.group(1).upper()
	except Exception:
		pass
	return ""


def scan_pdf(pdf_path: Path, part_type: str, thresholds: Thresholds) -> List[Dict[str, object]]:
	results: List[Dict[str, object]] = []
	if fitz is None:
		print("PyMuPDF is not available; cannot scan:", pdf_path)
		return results
	try:
		doc = fitz.open(pdf_path)
	except Exception as exc:
		print(f"Failed to open PDF {pdf_path}: {exc}")
		return results
	cmd_root_guess = None
	for ancestor in pdf_path.parents:
		if ancestor.name.lower() == "cmd":
			cmd_root_guess = ancestor
			break
	try:
		for page in doc:
			text = page.get_text("text")
			if not text:
				continue
			if part_type.lower() not in text.lower():
				continue
			ap_vals = _parse_three_numbers(RE_AP.search(text))
			ml_vals = _parse_three_numbers(RE_ML.search(text))
			maxh_vals = _parse_three_numbers(RE_MAX_H.search(text))
			if not (ap_vals and ml_vals and maxh_vals):
				continue
			implant_names = RE_IMPLANT.findall(text)
			lot_match = RE_LOT.search(text)
			lot = lot_match.group(0) if lot_match else ""
			po = _extract_po_from_path(pdf_path, cmd_root_guess or pdf_path.parent)
			for idx in (0, 1, 2):
				ap_v = ap_vals[idx]
				ml_v = ml_vals[idx]
				maxh_v = maxh_vals[idx]
				ok = True
				if thresholds.ap_depth_b_mm is not None and ap_v < thresholds.ap_depth_b_mm:
					ok = False
				if thresholds.ml_width_a_mm is not None and ml_v < thresholds.ml_width_a_mm:
					ok = False
				if thresholds.max_cage_height_c_mm is not None and maxh_v < thresholds.max_cage_height_c_mm:
					ok = False
				if not ok:
					continue
				implant_name = implant_names[idx] if idx < len(implant_names) else ""
				results.append({
					"PO": po,
					"Lot": lot,
					"PartType": part_type,
					"Plan": PLAN_LABELS.get(idx, str(idx)),
					"ImplantName": implant_name,
					"AP_Depth_B_mm": ap_v,
					"ML_Width_A_mm": ml_v,
					"Max_Cage_Height_C_mm": maxh_v,
					"PDF": str(pdf_path),
				})
	finally:
		doc.close()
	return results


def results_to_csv(rows: Sequence[Dict[str, object]]) -> str:
	fieldnames = [
		"PO",
		"Lot",
		"PartType",
		"Plan",
		"ImplantName",
		"AP_Depth_B_mm",
		"ML_Width_A_mm",
		"Max_Cage_Height_C_mm",
		"PDF",
	]
	buf = io.StringIO()
	writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
	writer.writeheader()
	for row in rows:
		writer.writerow(row)
	return buf.getvalue()


TEMPLATE = """
<!doctype html>
<html>
<head>
	<meta charset="utf-8" />
	<title>FORM-019 Finder</title>
	<style>
		body { font-family: Arial, sans-serif; margin: 20px; }
		label { display: inline-block; margin: 6px 0; }
		input[type=text], select, input[type=number] { padding: 6px; width: 320px; }
		fieldset { border: 1px solid #ccc; padding: 10px 14px; margin-bottom: 12px; }
		legend { padding: 0 6px; }
		button { padding: 8px 14px; }
		table { border-collapse: collapse; width: 100%; margin-top: 16px; }
		th, td { border: 1px solid #ccc; padding: 6px 8px; text-align: left; }
		th { background: #f2f2f2; }
		.small { color: #666; font-size: 12px; }
	</style>
</head>
<body>
	<h2>FORM-019 Finder</h2>
	<form method="post" action="/">
		<fieldset>
			<legend>Search Scope</legend>
			<label>Base Folder:<br>
				<input type="text" name="base_dir" value="{{ base_dir }}" />
			</label>
			<div>
				<label><input type="checkbox" name="ren" value="Ren A" {% if 'Ren A' in selected_rens %}checked{% endif %}> Ren A</label>
				<label><input type="checkbox" name="ren" value="Ren B" {% if 'Ren B' in selected_rens %}checked{% endif %}> Ren B</label>
				<label><input type="checkbox" name="ren" value="Ren C" {% if 'Ren C' in selected_rens %}checked{% endif %}> Ren C</label>
			</div>
		</fieldset>
		<fieldset>
			<legend>Filters</legend>
			<label>Part Type:<br>
				<select name="part_type">
					{% for pt in part_types %}
						<option value="{{ pt }}" {% if pt == part_type %}selected{% endif %}>{{ pt }}</option>
					{% endfor %}
				</select>
			</label>
			<div>
				<label>AP Depth “B” (mm) ≥<br>
					<input type="number" step="0.01" name="ap_b" value="{{ ap_b|default('') }}" />
				</label>
			</div>
			<div>
				<label>ML Width “A” (mm) ≥<br>
					<input type="number" step="0.01" name="ml_a" value="{{ ml_a|default('') }}" />
				</label>
			</div>
			<div>
				<label>Max Cage Height “C” (mm) ≥<br>
					<input type="number" step="0.01" name="max_c" value="{{ max_c|default('') }}" />
				</label>
			</div>
		</fieldset>
		<button type="submit">Search</button>
	</form>
	{% if results is defined %}
		<h3>Matches: {{ results|length }}</h3>
		{% if results|length > 0 %}
			<table>
				<thead>
					<tr>
						<th>PO</th>
						<th>Lot</th>
						<th>PartType</th>
						<th>Plan</th>
						<th>ImplantName</th>
						<th>AP_Depth_B_mm</th>
						<th>ML_Width_A_mm</th>
						<th>Max_Cage_Height_C_mm</th>
						<th>PDF</th>
					</tr>
				</thead>
				<tbody>
					{% for r in results %}
					<tr>
						<td>{{ r.PO }}</td>
						<td>{{ r.Lot }}</td>
						<td>{{ r.PartType }}</td>
						<td>{{ r.Plan }}</td>
						<td>{{ r.ImplantName }}</td>
						<td>{{ r.AP_Depth_B_mm }}</td>
						<td>{{ r.ML_Width_A_mm }}</td>
						<td>{{ r.Max_Cage_Height_C_mm }}</td>
						<td class="small">{{ r.PDF }}</td>
					</tr>
					{% endfor %}
				</tbody>
			</table>
			<form method="post" action="/download">
				<textarea name="csv" style="display:none;">{{ csv_data }}</textarea>
				<button type="submit">Download CSV</button>
			</form>
		{% endif %}
	{% endif %}
</body>
</html>
"""


def _parse_float(value: str) -> Optional[float]:
	value = (value or "").strip()
	if value == "":
		return None
	try:
		return float(value)
	except ValueError:
		return None


def _collect_results(base_dir: Path, selected_rens: Sequence[str], part_type: str, thresholds: Thresholds) -> List[Dict[str, object]]:
	results: List[Dict[str, object]] = []
	for cmd_dir in iter_job_cmd_dirs(base_dir, selected_rens):
		pdfs = find_form19_pdfs(cmd_dir)
		for pdf in pdfs:
			rows = scan_pdf(pdf, part_type, thresholds)
			results.extend(rows)
	return results


@app.get("/")
def index() -> str:
	return render_template_string(
		TEMPLATE,
		base_dir=DEFAULT_BASE_DIR,
		selected_rens=REN_FOLDERS,
		part_types=PART_TYPES,
		part_type=PART_TYPES[0],
	)


@app.post("/")
def search() -> str:
	base_dir_str = request.form.get("base_dir", DEFAULT_BASE_DIR)
	selected_rens = request.form.getlist("ren") or REN_FOLDERS
	part_type = request.form.get("part_type", PART_TYPES[0])
	ap_b = request.form.get("ap_b", "").strip()
	ml_a = request.form.get("ml_a", "").strip()
	max_c = request.form.get("max_c", "").strip()
	thresholds = Thresholds(
		ap_depth_b_mm=_parse_float(ap_b),
		ml_width_a_mm=_parse_float(ml_a),
		max_cage_height_c_mm=_parse_float(max_c),
	)
	base_dir = Path(base_dir_str)
	results = _collect_results(base_dir, selected_rens, part_type, thresholds)
	csv_data = results_to_csv(results)
	return render_template_string(
		TEMPLATE,
		base_dir=str(base_dir),
		selected_rens=selected_rens,
		part_types=PART_TYPES,
		part_type=part_type,
		ap_b=ap_b,
		ml_a=ml_a,
		max_c=max_c,
		results=results,
		csv_data=csv_data,
	)


@app.post("/download")
def download_csv() -> Response:
	csv_data = request.form.get("csv", "")
	response = Response(csv_data, mimetype="text/csv; charset=utf-8")
	response.headers["Content-Disposition"] = "attachment; filename=form019_matches.csv"
	return response


if __name__ == "__main__":
	app.run(host="127.0.0.1", port=5000, debug=True)
