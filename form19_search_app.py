from __future__ import annotations

import csv
import io
import re
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from flask import Flask, request, render_template_string, Response, jsonify
from threading import Thread, Lock

try:
	import fitz
except Exception as e:
	fitz = None
	print("Warning: PyMuPDF (fitz) failed to import:", e)


app = Flask(__name__)

DEFAULT_BASE_DIR = r"P:\\Client Data\\Carlsmed\\~Purchase Orders"
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
	ap_op: str
	ml_width_a_mm: Optional[float]
	ml_op: str
	max_cage_height_c_mm: Optional[float]
	max_op: str


def iter_po_dirs(base_dir: Path) -> Iterable[Path]:
	try:
		if not base_dir.is_dir():
			return
		for po_dir in base_dir.iterdir():
			if po_dir.is_dir():
				yield po_dir
	except Exception as exc:
		print(f"Error scanning base folder {base_dir}: {exc}")


def find_form19_pdfs(root_path: Path) -> List[Path]:
	pdfs: List[Path] = []
	try:
		for p in root_path.rglob("*.pdf"):
			name_lower = p.name.lower()
			if ("form-019" in name_lower) or ("form-19" in name_lower):
				pdfs.append(p)
	except Exception as exc:
		print(f"Error while searching PDFs in {root_path}: {exc}")
	return pdfs


def _parse_three_numbers(match: Optional[re.Match]) -> Optional[Tuple[float, float, float]]:
	if not match:
		return None
	try:
		return (float(match.group(1)), float(match.group(2)), float(match.group(3)))
	except Exception:
		return None


def _extract_po_from_path(pdf_path: Path, stop_at: Optional[Path] = None) -> str:
	try:
		for parent in [pdf_path.parent, *pdf_path.parents]:
			if stop_at is not None and parent == stop_at:
				break
			m = RE_PO.search(parent.name)
			if m:
				return m.group(1).upper()
	except Exception:
		pass
	return ""


def _cmp(value: float, threshold: Optional[float], op: str) -> bool:
	if threshold is None:
		return True
	if op == "<=":
		return value <= threshold
	return value >= threshold


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
			po = _extract_po_from_path(pdf_path, None)
			for idx in (0, 1, 2):
				ap_v = ap_vals[idx]
				ml_v = ml_vals[idx]
				maxh_v = maxh_vals[idx]
				ok = True
				if not _cmp(ap_v, thresholds.ap_depth_b_mm, thresholds.ap_op):
					ok = False
				if not _cmp(ml_v, thresholds.ml_width_a_mm, thresholds.ml_op):
					ok = False
				if not _cmp(maxh_v, thresholds.max_cage_height_c_mm, thresholds.max_op):
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


# Background job store
JOBS: Dict[str, Dict[str, object]] = {}
JOBS_LOCK = Lock()


def _gather_pdfs(base_dir: Path) -> List[Path]:
	pdfs: List[Path] = []
	for po_dir in iter_po_dirs(base_dir):
		pdfs.extend(find_form19_pdfs(po_dir))
	return pdfs


def _run_job(job_id: str, base_dir: Path, part_type: str, thresholds: Thresholds) -> None:
	try:
		pdfs = _gather_pdfs(base_dir)
		with JOBS_LOCK:
			JOBS[job_id]["total"] = len(pdfs)
		processed = 0
		results: List[Dict[str, object]] = []
		for pdf in pdfs:
			rows = scan_pdf(pdf, part_type, thresholds)
			results.extend(rows)
			processed += 1
			with JOBS_LOCK:
				JOBS[job_id]["processed"] = processed
		csv_data = results_to_csv(results)
		with JOBS_LOCK:
			JOBS[job_id]["results"] = results
			JOBS[job_id]["csv"] = csv_data
			JOBS[job_id]["done"] = True
	except Exception as exc:
		with JOBS_LOCK:
			JOBS[job_id]["error"] = str(exc)
			JOBS[job_id]["done"] = True


TEMPLATE = """
<!doctype html>
<html>
<head>
	<meta charset="utf-8" />
	<title>FORM-019 Finder</title>
	<style>
		:root { --radius: 8px; --border: #d0d5dd; --focus: #4f8cff; }
		* { box-sizing: border-box; }
		body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif; margin: 20px; color: #111827; }
		label { display: block; margin: 10px 0 6px; font-weight: 600; }
		input[type=text], select, input[type=number] { padding: 8px 10px; height: 36px; border-radius: var(--radius); border: 1px solid var(--border); background: #fff; outline: none; }
		input[type=text].wide { width: min(900px, 100%); }
		select.wide { width: 320px; max-width: 100%; }
		.inline { display: flex; gap: 8px; align-items: center; }
		.inline .op { width: 64px; text-align: center; }
		.inline .num { width: 180px; }
		fieldset { border: 1px solid #e5e7eb; padding: 14px 16px; margin-bottom: 14px; border-radius: var(--radius); background: #fafafa; }
		legend { padding: 0 6px; font-weight: 700; }
		button { padding: 9px 14px; border-radius: var(--radius); border: 1px solid var(--border); background: #111827; color: #fff; cursor: pointer; }
		button:hover { background: #0b1220; }
		input:focus, select:focus { border-color: var(--focus); box-shadow: 0 0 0 3px rgba(79,140,255,0.2); }
		table { border-collapse: collapse; width: 100%; margin-top: 16px; }
		th, td { border: 1px solid #e5e7eb; padding: 8px 10px; text-align: left; }
		th { background: #f9fafb; }
		.small { color: #6b7280; font-size: 12px; }
		.progress { width: 100%; background: #eee; height: 14px; border-radius: 7px; overflow: hidden; margin: 10px 0; }
		.bar { height: 100%; background: #4caf50; width: 0%; transition: width 0.2s; }
	</style>
</head>
<body>
	<h2>FORM-019 Finder</h2>
	<form id="searchForm" method="post" action="/start">
		<fieldset>
			<legend>Search Scope</legend>
			<label>Base Folder</label>
			<input class="wide" type="text" name="base_dir" value="{{ base_dir }}" />
		</fieldset>
		<fieldset>
			<legend>Filters</legend>
			<label>Part Type</label>
			<select class="wide" name="part_type">
				{% for pt in part_types %}
					<option value="{{ pt }}" {% if pt == part_type %}selected{% endif %}>{{ pt }}</option>
				{% endfor %}
			</select>
			<label>AP Depth “B” (mm)</label>
			<div class="inline">
				<select class="op" name="ap_op"><option value=">=" selected>≥</option><option value="<=">≤</option></select>
				<input class="num" type="number" step="0.01" name="ap_b" value="{{ ap_b|default('') }}" />
			</div>
			<label>ML Width “A” (mm)</label>
			<div class="inline">
				<select class="op" name="ml_op"><option value=">=" selected>≥</option><option value="<=">≤</option></select>
				<input class="num" type="number" step="0.01" name="ml_a" value="{{ ml_a|default('') }}" />
			</div>
			<label>Max Cage Height “C” (mm)</label>
			<div class="inline">
				<select class="op" name="max_op"><option value=">=" selected>≥</option><option value="<=">≤</option></select>
				<input class="num" type="number" step="0.01" name="max_c" value="{{ max_c|default('') }}" />
			</div>
		</fieldset>
		<button type="submit">Search</button>
	</form>

	<div id="progressWrap" style="display:none;">
		<div class="progress"><div id="bar" class="bar"></div></div>
		<div id="progressText">Starting…</div>
	</div>

	<div id="resultsWrap"></div>

	<script>
	(function() {
		const form = document.getElementById('searchForm');
		const progressWrap = document.getElementById('progressWrap');
		const bar = document.getElementById('bar');
		const progressText = document.getElementById('progressText');
		const resultsWrap = document.getElementById('resultsWrap');
		let pollTimer = null;

		function renderTable(rows) {
			if (!rows || rows.length === 0) { resultsWrap.innerHTML = '<p>No matches.</p>'; return; }
			let html = '<table><thead><tr>'+
				'<th>PO</th><th>Lot</th><th>PartType</th><th>Plan</th><th>ImplantName</th>'+
				'<th>AP_Depth_B_mm</th><th>ML_Width_A_mm</th><th>Max_Cage_Height_C_mm</th><th>PDF</th>'+
				'</tr></thead><tbody>';
			for (const r of rows) {
				html += '<tr>'+
					`<td>${r.PO||''}</td>`+
					`<td>${r.Lot||''}</td>`+
					`<td>${r.PartType||''}</td>`+
					`<td>${r.Plan||''}</td>`+
					`<td>${r.ImplantName||''}</td>`+
					`<td>${r.AP_Depth_B_mm||''}</td>`+
					`<td>${r.ML_Width_A_mm||''}</td>`+
					`<td>${r.Max_Cage_Height_C_mm||''}</td>`+
					`<td class="small">${r.PDF||''}</td>`+
					'</tr>';
			}
			html += '</tbody></table>';
			resultsWrap.innerHTML = html;
		}

		form.addEventListener('submit', async function(e) {
			e.preventDefault();
			resultsWrap.innerHTML = '';
			bar.style.width = '0%';
			progressText.textContent = 'Starting…';
			progressWrap.style.display = '';
			const formData = new FormData(form);
			const startRes = await fetch('/start', { method: 'POST', body: formData });
			const startData = await startRes.json();
			const jobId = startData.job_id;
			if (!jobId) { progressText.textContent = 'Error starting job.'; return; }
			if (pollTimer) clearInterval(pollTimer);
			pollTimer = setInterval(async () => {
				const progRes = await fetch(`/progress/${jobId}`);
				const prog = await progRes.json();
				const total = prog.total || 0, processed = prog.processed || 0;
				const pct = total > 0 ? Math.floor(processed * 100 / total) : 0;
				bar.style.width = pct + '%';
				progressText.textContent = prog.done ? 'Finalizing…' : `Scanning PDFs: ${processed} / ${total}`;
				if (prog.done) {
					clearInterval(pollTimer);
					const resRes = await fetch(`/results/${jobId}`);
					const data = await resRes.json();
					renderTable(data.results || []);
					const dl = document.createElement('a');
					dl.textContent = 'Download CSV';
					dl.href = `/download/${jobId}`;
					dl.setAttribute('download', 'form019_matches.csv');
					resultsWrap.prepend(dl);
					progressText.textContent = `Done. ${processed} PDFs scanned.`;
				}
			}, 500);
		});
	})();
	</script>
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


@app.get("/")
def index() -> str:
	return render_template_string(
		TEMPLATE,
		base_dir=DEFAULT_BASE_DIR,
		part_types=PART_TYPES,
		part_type=PART_TYPES[0],
	)


@app.post("/start")
def start() -> Response:
	base_dir_str = request.form.get("base_dir", DEFAULT_BASE_DIR)
	part_type = request.form.get("part_type", PART_TYPES[0])
	ap_b = request.form.get("ap_b", "").strip()
	ml_a = request.form.get("ml_a", "").strip()
	max_c = request.form.get("max_c", "").strip()
	ap_op = request.form.get("ap_op", ">=")
	ml_op = request.form.get("ml_op", ">=")
	max_op = request.form.get("max_op", ">=")
	thresholds = Thresholds(
		ap_depth_b_mm=_parse_float(ap_b),
		ap_op=ap_op if ap_op in (">=", "<=") else ">=",
		ml_width_a_mm=_parse_float(ml_a),
		ml_op=ml_op if ml_op in (">=", "<=") else ">=",
		max_cage_height_c_mm=_parse_float(max_c),
		max_op=max_op if max_op in (">=", "<=") else ">=",
	)
	base_dir = Path(base_dir_str)
	job_id = uuid.uuid4().hex
	with JOBS_LOCK:
		JOBS[job_id] = {"total": 0, "processed": 0, "done": False, "results": [], "csv": "", "error": None}
	thread = Thread(target=_run_job, args=(job_id, base_dir, part_type, thresholds), daemon=True)
	thread.start()
	return jsonify({"job_id": job_id})


@app.get("/progress/<job_id>")
def progress(job_id: str) -> Response:
	with JOBS_LOCK:
		info = JOBS.get(job_id)
		if not info:
			return jsonify({"error": "not_found"}), 404
		return jsonify({"total": info.get("total", 0), "processed": info.get("processed", 0), "done": info.get("done", False), "error": info.get("error")})


@app.get("/results/<job_id>")
def get_results(job_id: str) -> Response:
	with JOBS_LOCK:
		info = JOBS.get(job_id)
		if not info:
			return jsonify({"error": "not_found"}), 404
		return jsonify({"results": info.get("results", [])})


@app.get("/download/<job_id>")
def download_csv(job_id: str) -> Response:
	with JOBS_LOCK:
		info = JOBS.get(job_id)
		if not info:
			return Response("not found", status=404)
		csv_data = info.get("csv", "") or ""
	response = Response(csv_data, mimetype="text/csv; charset=utf-8")
	response.headers["Content-Disposition"] = "attachment; filename=form019_matches.csv"
	return response


if __name__ == "__main__":
	app.run(host="127.0.0.1", port=5000, debug=True)
