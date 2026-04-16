from django.shortcuts import render,redirect
from django.http import HttpResponse, JsonResponse, FileResponse
from django.conf import settings
import requests
import urllib3
from .utils import REFERENCE_KTR, REFERENCE_KJB, SYSTEM_PROMPT_KTR, SYSTEM_PROMPT_KJB, call_claude, execute_sql, _SYSTEM_PROMPT,SYSTEM_PROMPT_SQL_PRIVIEW
import json
import psycopg2
from django.views.decorators.csrf  import csrf_exempt
from django.views.decorators.http  import require_http_methods,require_GET,require_POST
import zipfile
from io import BytesIO
import re
from typing import TypedDict
from .models import GenerationLog, GeneratedFile, UserApiSettings, UserPreference 
import time
from django.db.models import Avg, Count, Q, Sum
from django.utils import timezone
from datetime import timedelta
import os
import sqlalchemy
import pyodbc
import oracledb
import pymysql
from django.contrib.auth.decorators import login_required
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.forms import AuthenticationForm
from django.contrib import messages
import os, zipfile, io
from django.core.files.base import ContentFile
from decouple import config
import vosk
import queue
from django.db import connection


def home(request):
    """
    Root landing page — two platform selector cards.
    If already authenticated, redirect to the platform they last used.
    """
    if request.user.is_authenticated:
        platform = request.session.get('platform', 'pentaho')
        if platform == 'talend':
            return redirect('talend_generator')
        return redirect('dashboard')
 
    return render(request, 'home.html')



# ================= Transformation file function =================
@login_required(login_url='/')
def upload_tr(request):
    if request.method == 'POST':
        uploaded_file = request.FILES.get('steps_file')

        if not uploaded_file:
            return JsonResponse({"error": "No file uploaded"}, status=400)

        try:
            # ================= CONFIGURATION =================
            settings_obj = UserApiSettings.objects.last()

            model = settings_obj.model
            max_tokens = settings_obj.max_tokens
            timeout = settings_obj.timeout
            api_key = settings_obj.api_key
            output_file = "C:/Users/prabhakaranp/Downloads/generated_transformation.ktr"

            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

            # ================= READ INSTRUCTIONS =================
            instructions = uploaded_file.read().decode('utf-8')

            # ================= REFERENCE KTR TEMPLATE =================
            reference_ktr = REFERENCE_KTR

            # ================= CALL API =================
            url = "https://api.anthropic.com/v1/messages"

            headers = {
                "x-api-key": api_key,
                "Content-Type": "application/json",
                "anthropic-version": "2023-06-01"
            }

            system_prompt = SYSTEM_PROMPT_KTR

            user_prompt = f"""Generate a complete Pentaho .ktr transformation XML based on these instructions:

            --- INSTRUCTIONS ---
            {instructions}

            --- REFERENCE FORMAT (follow this exact XML structure) ---
            {reference_ktr}

            Output ONLY the raw XML starting with: <?xml version="1.0" encoding="UTF-8"?>
            """

            data = {
                "model": model,
                "max_tokens": max_tokens,
                "system": system_prompt,
                "messages": [
                    {
                        "role": "user",
                        "content": user_prompt
                    }
                ]
            }

            t0 = int(time.time() * 1000)                          # ← log: start timer
            response = requests.post(url, headers=headers, json=data, verify=False)

            if response.status_code == 200:
                body = response.json()
                ktr_xml = body["content"][0]["text"].strip()

                # Remove markdown if exists
                if ktr_xml.startswith("```"):
                    ktr_xml = ktr_xml.split("\n", 1)[1]
                if ktr_xml.endswith("```"):
                    ktr_xml = ktr_xml.rsplit("```", 1)[0]

                ktr_xml = ktr_xml.strip()

                # Save file locally
                with open(output_file, "w", encoding="utf-8") as f:
                    f.write(ktr_xml)
                
                gf = GeneratedFile(
                name="generated_transformation",
                file_type="ktr",
                size_bytes=len(ktr_xml.encode("utf-8")),
                source="upload_tr",
                )
                gf.file.save("generated_transformation.ktr", ContentFile(ktr_xml), save=True)

                # ── LOG success ──────────────────────────────────────────
                usage = body.get("usage", {})
                GenerationLog.objects.create(
                    filename      = "generated_transformation.ktr",
                    file_type     = "ktr",
                    status        = "ok",
                    step_count    = ktr_xml.count("<step>"),
                    input_tokens  = usage.get("input_tokens", 0),
                    output_tokens = usage.get("output_tokens", 0),
                    duration_ms   = int(time.time() * 1000) - t0,
                )
                # ────────────────────────────────────────────────────────

                django_response = HttpResponse(ktr_xml, content_type='application/xml')
                django_response['Content-Disposition'] = 'attachment; filename="generated_transformation.ktr"'
                return django_response

            else:
                # ── LOG failure ──────────────────────────────────────────
                GenerationLog.objects.create(
                    filename    = "generated_transformation.ktr",
                    file_type   = "ktr",
                    status      = "error",
                    error_message = f"API Error {response.status_code}: {response.text[:500]}",
                    duration_ms = int(time.time() * 1000) - t0,
                )
                # ────────────────────────────────────────────────────────
                return JsonResponse({"error": f"API Error {response.status_code}: {response.text}"}, status=400)

        except Exception as e:
            # ── LOG exception ────────────────────────────────────────────
            GenerationLog.objects.create(
                filename      = "generated_transformation.ktr",
                file_type     = "ktr",
                status        = "error",
                error_message = str(e)[:500],
            )
            # ────────────────────────────────────────────────────────────
            return JsonResponse({"error": f"Error: {str(e)}"}, status=400)
    model_name = (settings.ANTHROPIC_MODEL or "—").replace("claude-", "")
    return render(request, "tr_upload.html", {
        "active_page": "upload_tr","model": model_name,   # ← pass to template
    })


# ================= Job file function =================
@login_required(login_url='/')
def upload_job(request):
    if request.method == 'POST':
        uploaded_file = request.FILES.get('steps_file')

        if not uploaded_file:
            return JsonResponse({"error": "No file uploaded"}, status=400)

        try:
            # ================= CONFIGURATION =================
            settings_obj = UserApiSettings.objects.last()

            model = settings_obj.model
            max_tokens = settings_obj.max_tokens
            timeout = settings_obj.timeout
            api_key = settings_obj.api_key
            output_file = "C:/Users/prabhakaranp/Downloads/generated_job.kjb"

            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

            # ================= READ INSTRUCTIONS =================
            instructions = uploaded_file.read().decode('utf-8')

            # ================= REFERENCE KJB TEMPLATE =================
            reference_kjb = REFERENCE_KJB

            # ================= CALL API =================
            url = "https://api.anthropic.com/v1/messages"

            headers = {
                "x-api-key": api_key,
                "Content-Type": "application/json",
                "anthropic-version": "2023-06-01"
            }

            system_prompt = SYSTEM_PROMPT_KJB

            user_prompt = f"""Generate a complete Pentaho .kjb job XML based on these instructions:

            --- INSTRUCTIONS ---
            {instructions}

            --- REFERENCE FORMAT (follow this exact XML structure) ---
            {reference_kjb}

            Output ONLY the raw XML starting with: <?xml version="1.0" encoding="UTF-8"?>
            """

            data = {
                "model": model,
                "max_tokens": max_tokens,
                "system": system_prompt,
                "messages": [
                    {
                        "role": "user",
                        "content": user_prompt
                    }
                ]
            }

            t0 = int(time.time() * 1000)                          # ← log: start timer
            response = requests.post(url, headers=headers, json=data, verify=False)

            if response.status_code == 200:
                body = response.json()
                kjb_xml = body["content"][0]["text"].strip()

                # Remove markdown if exists
                if kjb_xml.startswith("```"):
                    kjb_xml = kjb_xml.split("\n", 1)[1]
                if kjb_xml.endswith("```"):
                    kjb_xml = kjb_xml.rsplit("```", 1)[0]

                kjb_xml = kjb_xml.strip()

                # Save file locally
                with open(output_file, "w", encoding="utf-8") as f:
                    f.write(kjb_xml)

                # ── LOG success ──────────────────────────────────────────
                usage = body.get("usage", {})
                GenerationLog.objects.create(
                    filename      = "generated_job.kjb",
                    file_type     = "kjb",
                    status        = "ok",
                    step_count    = kjb_xml.count("<entry>"),
                    input_tokens  = usage.get("input_tokens", 0),
                    output_tokens = usage.get("output_tokens", 0),
                    duration_ms   = int(time.time() * 1000) - t0,
                )
                # ────────────────────────────────────────────────────────

                django_response = HttpResponse(kjb_xml, content_type='application/xml')
                django_response['Content-Disposition'] = 'attachment; filename="generated_job.kjb"'
                return django_response

            else:
                # ── LOG failure ──────────────────────────────────────────
                GenerationLog.objects.create(
                    filename      = "generated_job.kjb",
                    file_type     = "kjb",
                    status        = "error",
                    error_message = f"API Error {response.status_code}: {response.text[:500]}",
                    duration_ms   = int(time.time() * 1000) - t0,
                )
                # ────────────────────────────────────────────────────────
                return JsonResponse({"error": f"API Error {response.status_code}: {response.text}"}, status=400)

        except Exception as e:
            # ── LOG exception ────────────────────────────────────────────
            GenerationLog.objects.create(
                filename      = "generated_job.kjb",
                file_type     = "kjb",
                status        = "error",
                error_message = str(e)[:500],
            )
            # ────────────────────────────────────────────────────────────
            return JsonResponse({"error": f"Error: {str(e)}"}, status=400)
        
    model_name = (settings.ANTHROPIC_MODEL or "—").replace("claude-", "")
    return render(request, "tr_upload.html", {
        "model": model_name,   # ← pass to template
    })


# ── table_builder ─────────────────────────────────────────────────────────────
@login_required(login_url='/')
def table_builder(request):
    """Render the main UI page."""
    model_name = (settings.ANTHROPIC_MODEL or "—").replace("claude-", "")
    return render(request, "table_create_ui.html", {
        "active_page": "table_builder","model": model_name,   # ← pass to template
    })


# ── Generate SQL ──────────────────────────────────────────────────────────────
@login_required(login_url='/')
@csrf_exempt
@require_http_methods(["POST"])
def generate_sql(request):
    if "file" not in request.FILES:
        return JsonResponse({"error": "No file uploaded"}, status=400)

    uploaded_file = request.FILES["file"]

    try:
        instructions = uploaded_file.read().decode("utf-8")
    except UnicodeDecodeError:
        return JsonResponse({"error": "File must be a UTF-8 text file"}, status=400)

    if not instructions.strip():
        return JsonResponse({"error": "Instruction file is empty"}, status=400)

    t0 = int(time.time() * 1000)
    try:
        sql, in_tok, out_tok = call_claude(instructions)   # ← unpack 3 values now
    except RuntimeError as e:
        GenerationLog.objects.create(
            filename      = uploaded_file.name or "query.sql",
            file_type     = "sql",
            status        = "error",
            error_message = str(e)[:500],
            duration_ms   = int(time.time() * 1000) - t0,
        )
        return JsonResponse({"error": str(e)}, status=500)

    GenerationLog.objects.create(
        filename      = uploaded_file.name or "query.sql",
        file_type     = "sql",
        status        = "ok",
        input_tokens  = in_tok,    # ← now populated
        output_tokens = out_tok,   # ← now populated
        duration_ms   = int(time.time() * 1000) - t0,
    )

    return JsonResponse({
        "sql":          sql,
        "instructions": instructions,
    })


# ── Create Table ──────────────────────────────────────────────────────────────
@login_required(login_url='/')
@csrf_exempt
@require_http_methods(["POST"])
def create_table(request):
    try:
        body = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON body"}, status=400)

    sql = body.get("sql", "").strip()
    if not sql:
        return JsonResponse({"error": "No SQL provided"}, status=400)

    t0 = int(time.time() * 1000)                                  # ← log: start timer
    try:
        result = execute_sql(sql)
    except psycopg2.Error as e:
        # ── LOG failure ──────────────────────────────────────────────
        GenerationLog.objects.create(
            filename      = "create_table",
            file_type     = "sql",
            status        = "error",
            error_message = str(e)[:500],
            duration_ms   = int(time.time() * 1000) - t0,
        )
        # ────────────────────────────────────────────────────────────
        return JsonResponse({"error": f"PostgreSQL error: {e}"}, status=500)
    except Exception as e:
        # ── LOG failure ──────────────────────────────────────────────
        GenerationLog.objects.create(
            filename      = "create_table",
            file_type     = "sql",
            status        = "error",
            error_message = str(e)[:500],
            duration_ms   = int(time.time() * 1000) - t0,
        )
        # ────────────────────────────────────────────────────────────
        return JsonResponse({"error": f"Unexpected error: {e}"}, status=500)

    # ── LOG success ──────────────────────────────────────────────────
    GenerationLog.objects.create(
        filename    = result["table_name"],
        file_type   = "sql",
        status      = "ok",
        duration_ms = int(time.time() * 1000) - t0,
    )
    # ────────────────────────────────────────────────────────────────

    return JsonResponse({
        "success":    True,
        "table_name": result["table_name"],
        "columns":    result["columns"],
        "message":    f"Table '{result['table_name']}' created successfully!",
    })


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ── Types ─────────────────────────────────────────────────────────────────────

class PentahoFile(TypedDict):
    name:     str
    filename: str
    xml:      str


class GenerateResult(TypedDict):
    transformations: list[PentahoFile]
    jobs:            list[PentahoFile]
    raw_response:    str


# ── Internal helpers ──────────────────────────────────────────────────────────

def _parse_delimited_output(text: str) -> tuple[list[PentahoFile], list[PentahoFile]]:
    transformations: list[PentahoFile] = []
    jobs:            list[PentahoFile] = []

    pattern = re.compile(
        r"##(KTR|KJB):([^#]+)##\s*(.*?)\s*##END##",
        re.DOTALL | re.IGNORECASE,
    )

    for match in pattern.finditer(text):
        file_type = match.group(1).upper()
        name      = match.group(2).strip()
        xml       = match.group(3).strip()

        if not xml.startswith("<?xml"):
            xml = '<?xml version="1.0" encoding="UTF-8"?>\n' + xml

        ext: str = "ktr" if file_type == "KTR" else "kjb"
        entry: PentahoFile = {
            "name":     name,
            "filename": f"{name}.{ext}",
            "xml":      xml,
        }

        if file_type == "KTR":
            transformations.append(entry)
        else:
            jobs.append(entry)

    return transformations, jobs


def _call_claude(instructions: str) -> tuple[str, int, int]:
    """
    Send instructions to Claude.
    Returns (raw_text, input_tokens, output_tokens).
    Raises RuntimeError on non-200 API responses.
    Raises requests.exceptions.RequestException on network failure.
    """
    user_prompt = (
        "Read the following instructions carefully.\n"
        "Identify every transformation and job described.\n"
        "Generate ALL of them using the delimiter format.\n\n"
        f"--- INSTRUCTIONS ---\n{instructions}\n---\n\n"
        "Remember: use ##KTR:name## ... ##END## and ##KJB:name## ... ##END## only."
    )

    settings_obj = UserApiSettings.objects.last()

    model = settings_obj.model
    max_tokens = settings_obj.max_tokens
    timeout = settings_obj.timeout
    api_key = settings_obj.api_key

    response = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key":         api_key,
            "anthropic-version": "2023-06-01",
            "Content-Type":      "application/json",
        },
        json={
            "model":      model,
            "max_tokens": max_tokens, #16000
            "system":     _SYSTEM_PROMPT,
            "messages":   [{"role": "user", "content": user_prompt}],
        },
        timeout=timeout,
        verify=False,
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"Claude API error {response.status_code}: {response.text}"
        )

    body          = response.json()
    text          = body["content"][0]["text"].strip()
    usage         = body.get("usage", {})
    input_tokens  = usage.get("input_tokens", 0)               # ← now returned
    output_tokens = usage.get("output_tokens", 0)              # ← now returned
    return text, input_tokens, output_tokens


def _build_zip(transformations: list[PentahoFile], jobs: list[PentahoFile]) -> BytesIO:
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for tr in transformations:
            zf.writestr(tr["filename"], tr["xml"])
        for job in jobs:
            zf.writestr(job["filename"], job["xml"])
    buf.seek(0)
    return buf


# ── Views ─────────────────────────────────────────────────────────────────────
@login_required(login_url='/')
def generate_pentaho(request):
    """GET /generate_pentaho/ — render the generator UI."""
    model_name = (settings.ANTHROPIC_MODEL or "—").replace("claude-", "")
    return render(request, "pentaho_generator.html", {
        "active_page": "File_Generator","model": model_name,   # ← pass to template
    })

@login_required(login_url='/')
@csrf_exempt
@require_http_methods(["POST"])
def generate_pentaho_files(request):
    # ── 1. Read input ─────────────────────────────────────────────────────
    instructions = ""
    prefs, _ = UserPreference.objects.get_or_create(id=1)
    folder_name = prefs.folder_name or "pentaho_files"
    auto_download = prefs.auto_download or True
   
    uploaded_file = request.FILES.get("steps_file")
    if uploaded_file:
        try:
            instructions = uploaded_file.read().decode("utf-8")
        except UnicodeDecodeError:
            return JsonResponse({"error": "Uploaded file must be UTF-8 encoded."}, status=400)
    else:
        instructions = request.POST.get("steps_text", "").strip()

    if not instructions:
        return JsonResponse(
            {"error": "No instructions provided. Send steps_file or steps_text."},
            status=400,
        )

    # ── 2. Call Claude ────────────────────────────────────────────────────
    t0 = int(time.time() * 1000)                                  # ← log: start timer
    try:
        raw_output, in_tok, out_tok = _call_claude(instructions)  # ← unpack tokens
    except requests.exceptions.RequestException as exc:
        # ── LOG failure ──────────────────────────────────────────────
        GenerationLog.objects.create(
            filename      = folder_name + ".zip",
            file_type     = "ktr",
            status        = "error",
            error_message = str(exc)[:500],
            duration_ms   = int(time.time() * 1000) - t0,
        )
        # ────────────────────────────────────────────────────────────
        return JsonResponse({"error": f"Network error calling Claude: {exc}"}, status=502)
    except RuntimeError as exc:
        # ── LOG failure ──────────────────────────────────────────────
        GenerationLog.objects.create(
            filename      = folder_name + ".zip",
            file_type     = "ktr",
            status        = "error",
            error_message = str(exc)[:500],
            duration_ms   = int(time.time() * 1000) - t0,
        )
        # ────────────────────────────────────────────────────────────
        return JsonResponse({"error": str(exc)}, status=500)

    duration_ms = int(time.time() * 1000) - t0

    # ── 3. Parse output ───────────────────────────────────────────────────
    transformations, jobs = _parse_delimited_output(raw_output)

    if not transformations and not jobs:
        # ── LOG parse failure ─────────────────────────────────────────
        GenerationLog.objects.create(
            filename      = folder_name + ".zip",
            file_type     = "ktr",
            status        = "error",
            error_message = "No files parsed from Claude output",
            input_tokens  = in_tok,
            output_tokens = out_tok,
            duration_ms   = duration_ms,
        )
        # ────────────────────────────────────────────────────────────
        return JsonResponse(
            {
                "error": "Claude returned output but no files could be parsed.",
                "debug": raw_output[:500],
            },
            status=500,
        )

    # ── LOG one row per generated file ────────────────────────────────────
    for tr in transformations:
        GenerationLog.objects.create(
            filename      = tr["filename"],
            file_type     = "ktr",
            status        = "ok",
            step_count    = tr["xml"].count("<step>"),
            input_tokens  = in_tok,
            output_tokens = out_tok,
            duration_ms   = duration_ms,
        )
    for job in jobs:
        GenerationLog.objects.create(
            filename      = job["filename"],
            file_type     = "kjb",
            status        = "ok",
            step_count    = job["xml"].count("<entry>"),
            input_tokens  = in_tok,
            output_tokens = out_tok,
            duration_ms   = duration_ms,
        )
    # ─────────────────────────────────────────────────────────────────────

    # ── 4. Build and return ZIP ───────────────────────────────────────────
    zip_buffer = _build_zip(transformations, jobs)

    # Convert ZIP to bytes
    zip_bytes = zip_buffer.getvalue()

    # 🔥 SAVE FILES TO MEDIA + DATABASE
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        for fname in z.namelist():

            if fname.endswith('/'):
                continue

            content = z.read(fname)
            ext = fname.split('.')[-1].lower()
            name = fname.rsplit('.', 1)[0]

            gf = GeneratedFile(
                name=name,
                file_type=ext,
                size_bytes=len(content),
                source="pentaho_generator",
            )

            gf.file.save(fname, ContentFile(content), save=True)

    response = HttpResponse(zip_buffer.read(), content_type="application/zip")
    response["Content-Disposition"] = f'attachment; filename="{folder_name}.zip"'
    response["X-KTR-Count"]                  = str(len(transformations))
    response["X-KJB-Count"]                  = str(len(jobs))
    response["Access-Control-Expose-Headers"] = "X-KTR-Count, X-KJB-Count"
    response["auto_download"] = auto_download
    return response

@login_required(login_url='/')
def dashboard(request):
    settings_obj = UserApiSettings.objects.last()

    model = settings_obj.model
    max_tokens = settings_obj.max_tokens
    timeout = settings_obj.timeout
    api_key = settings_obj.api_key

    return render(request, "dashboard.html", {
       "active_page": "dashboard", "api_key": api_key or "", "model": model, "max_tokens": max_tokens, "timeout": timeout
    })


# ── Dashboard stats API ───────────────────────────────────────────────────────
# @login_required(login_url='/')
# def dashboard_stats(request):
#     now       = timezone.now()
#     week_ago  = now - timedelta(days=7)
#     month_ago = now - timedelta(days=30)

#     qs       = GenerationLog.objects.all()
#     qs_week  = qs.filter(created_at__gte=week_ago)
#     qs_month = qs.filter(created_at__gte=month_ago)

#     total_calls  = qs.count()
#     calls_delta  = qs_week.count()
#     total_files  = qs.filter(status="ok").count()
#     files_delta  = qs_month.filter(status="ok").count()
#     fail_count   = qs.filter(status="error").count()
#     success_rate = round((total_files / total_calls * 100) if total_calls else 0, 1)

#     tok = qs.aggregate(tin=Sum("input_tokens"), tout=Sum("output_tokens"))
#     total_in_tok  = tok["tin"]  or 0
#     total_out_tok = tok["tout"] or 0

#     ktr_count = qs.filter(file_type="ktr", status="ok").count()
#     kjb_count = qs.filter(file_type="kjb", status="ok").count()

#     # Daily call counts — last 7 days
#     daily = []
#     for i in range(6, -1, -1):
#         day_start = now - timedelta(days=i+1)
#         day_end   = now - timedelta(days=i)
#         daily.append(qs.filter(created_at__gte=day_start, created_at__lt=day_end).count())

#     # Recent activity — last 20 records
#     activity = []
#     for row in qs.order_by("-created_at")[:20]:
#         activity.append({
#             "type":    row.file_type if row.status == "ok" else "err",
#             "name":    row.filename,
#             "time_ms": int(row.created_at.timestamp() * 1000),
#             "tokens":  row.total_tokens,          # uses model @property
#             "ok":      row.status == "ok",
#         })

#     # History table — last 50 records
#     history = []
#     for row in qs.order_by("-created_at")[:50]:
#         history.append({
#             "id":      row.pk,
#             "name":    row.filename,
#             "type":    row.file_type,
#             "steps":   row.step_count,
#             "tokens":  row.total_tokens,          # uses model @property
#             "dur":     row.duration_s,             # uses model @property  e.g. "1.2s"
#             "ok":      row.status == "ok",
#             "time_ms": int(row.created_at.timestamp() * 1000),
#         })

#     return JsonResponse({
#         "model":          getattr(settings, "ANTHROPIC_MODEL", ""),
#         "total_calls":    total_calls,
#         "calls_delta":    calls_delta,
#         "total_files":    total_files,
#         "files_delta":    files_delta,
#         "fail_count":     fail_count,
#         "success_rate":   success_rate,
#         "total_in_tok":   total_in_tok,
#         "total_out_tok":  total_out_tok,
#         "ktr_count":      ktr_count,
#         "kjb_count":      kjb_count,
#         "daily":          daily,
#         "activity":       activity,
#         "history":        history,
#     })



@login_required(login_url='/')
def dashboard_stats(request):
    now = timezone.now()

    settings_obj = UserApiSettings.objects.last()

    model = settings_obj.model
    max_tokens = settings_obj.max_tokens
    timeout = settings_obj.timeout
    api_key = settings_obj.api_key
    
    # Calculate time ranges
    week_ago  = now - timedelta(days=7)
    month_ago = now - timedelta(days=30)

    qs = GenerationLog.objects.all()
    qs_week  = qs.filter(created_at__gte=week_ago)
    qs_month = qs.filter(created_at__gte=month_ago)

    # Overall stats
    total_calls  = qs.count()
    calls_delta  = qs_week.count()
    total_files  = qs.filter(status="ok").count()
    files_delta  = qs_month.filter(status="ok").count()
    fail_count   = qs.filter(status="error").count()
    success_rate = round((total_files / total_calls * 100) if total_calls else 0, 1)

    # Token stats
    tok = qs.aggregate(tin=Sum("input_tokens"), tout=Sum("output_tokens"))
    total_in_tok  = tok["tin"]  or 0
    total_out_tok = tok["tout"] or 0

    # File type stats
    ktr_count = qs.filter(file_type="ktr", status="ok").count()
    kjb_count = qs.filter(file_type="kjb", status="ok").count()

    # Daily call counts — last 7 days (today included)
    daily = []
    for i in range(6, -1, -1):
        day = timezone.localdate() - timedelta(days=i)
        daily.append(qs.filter(created_at__date=day).count())

    # Recent activity — last 20 records
    activity = []
    for row in qs.order_by("-created_at")[:20]:
        activity.append({
            "type":    row.file_type if row.status == "ok" else "err",
            "name":    row.filename,
            "time_ms": int(row.created_at.timestamp() * 1000),
            "tokens":  row.total_tokens,  # @property
            "ok":      row.status == "ok",
        })

    # History table — last 50 records
    history = []
    prefs, _ = UserPreference.objects.get_or_create(id=1)
    history_limit = prefs.history_limit or 50
    for row in qs.order_by("-created_at")[:history_limit]:
        history.append({
            "id":      row.pk,
            "name":    row.filename,
            "type":    row.file_type,
            "steps":   row.step_count,
            "tokens":  row.total_tokens,  # @property
            "dur":     row.duration_s,    # @property, e.g., "1.2s"
            "ok":      row.status == "ok",
            "time_ms": int(row.created_at.timestamp() * 1000),
        })

    return JsonResponse({
        "model": model,
        "max_tokens" : max_tokens,
        "timeout" : timeout,
        "api_key" : api_key,
        "total_calls":    total_calls,
        "calls_delta":    calls_delta,
        "total_files":    total_files,
        "files_delta":    files_delta,
        "fail_count":     fail_count,
        "success_rate":   success_rate,
        "total_in_tok":   total_in_tok,
        "total_out_tok":  total_out_tok,
        "ktr_count":      ktr_count,
        "kjb_count":      kjb_count,
        "daily":          daily,
        "activity":       activity,
        "history":        history,
    })


@login_required(login_url='/')
def history_only(request):
    now = timezone.now()
    qs = GenerationLog.objects.all().order_by("-created_at")

    # History limit from preferences
    prefs, _ = UserPreference.objects.get_or_create(id=1)
    history_limit = prefs.history_limit or 50

    history = []
    for row in qs[:history_limit]:
        history.append({
            "id":      row.pk,
            "name":    row.filename,
            "type":    row.file_type,
            "steps":   row.step_count,
            "tokens":  row.total_tokens,  # @property
            "dur":     row.duration_s,    # @property
            "ok":      row.status == "ok",
            "time_ms": int(row.created_at.timestamp() * 1000),
        })

    return JsonResponse(history, safe=False)


@login_required(login_url='/')
@csrf_exempt
@require_http_methods(["POST"])
def dashboard_clear_history(request):
    """POST /dashboard/history/clear/ — delete all generation logs."""
    deleted, _ = GenerationLog.objects.all().delete()
    return JsonResponse({"deleted": deleted})


def _row_to_dict(r):
    """Serialize a GenerationLog instance for the API."""
    return {
        "id":      r.id,
        "name":    r.filename,
        "type":    r.file_type,                          # "ktr" | "kjb"
        "steps":   r.step_count,                         # ✅ was r.steps_count
        "tokens":  r.total_tokens,                       # ✅ was r.tokens_used (use property)
        "dur":     r.duration_s,                         # ✅ use the model property
        "ok":      r.status == "ok",                     # ✅ was r.success (no such field)
        "time_ms": int(r.created_at.timestamp() * 1000),
        "error":   r.error_message,
    }

@login_required(login_url='/')
@require_GET
def history_list(request):
    file_type = request.GET.get("type", "").lower().strip()
    page      = max(1, int(request.GET.get("page",  1)))
    limit     = min(100, int(request.GET.get("limit", 50)))
    search    = request.GET.get("q", "").strip()

    qs = GenerationLog.objects.all().order_by("-created_at")

    if file_type in ("ktr", "kjb"):
        qs = qs.filter(file_type=file_type)

    if search:
        qs = qs.filter(filename__icontains=search)

    total       = qs.count()
    total_pages = max(1, (total + limit - 1) // limit)
    offset      = (page - 1) * limit
    rows        = qs[offset : offset + limit]

    return JsonResponse({
        "total":       total,
        "page":        page,
        "limit":       limit,
        "total_pages": total_pages,
        "type":        file_type or "all",
        "search":      search,
        "rows":        [_row_to_dict(r) for r in rows],
    })

@login_required(login_url='/')
@require_GET
def history_detail(request, pk):
    try:
        r = GenerationLog.objects.get(pk=pk)
    except GenerationLog.DoesNotExist:
        return JsonResponse({"error": "Record not found"}, status=404)

    return JsonResponse({
        "id":         r.id,
        "name":       r.filename,
        "type":       r.file_type,
        "steps":      r.step_count,                      # ✅ correct field
        "tokens":     r.total_tokens,                    # ✅ use property
        "in_tokens":  r.input_tokens,
        "out_tokens": r.output_tokens,
        "dur":        r.duration_s,                      # ✅ use property
        "ok":         r.status == "ok",                  # ✅ derive from status field
        "time_ms":    int(r.created_at.timestamp() * 1000),
        "error":      r.error_message,
        "prompt_used": "",                               # ✅ field doesn't exist, return empty
        "xml_output":  "",                               # ✅ field doesn't exist, return empty
    })

# ── Add this view to your views.py ──────────────────────────────────────────
@login_required(login_url='/')
def table_join(request):
    """Renders the Table Join Builder page."""
    model = getattr(settings, "ANTHROPIC_MODEL", "") \
            or os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")
    return render(request, "table_join.html", {"model": model})

@login_required(login_url='/')
def table_joins(request):
    """Renders the Table Join Builder page."""
    model = getattr(settings, "ANTHROPIC_MODEL", "") \
            or os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")
    return render(request, "table_join_builders.html", {"active_page": "joins","model": model})

@login_required(login_url='/')
@require_POST
def test_db_connection(request):
    """
    POST /api/test-connection/
    Body: { type, host, port, db, schema, user, pass, jdbc }
    Returns: { success: bool, message: str, error: str }
    """
    try:
        data   = json.loads(request.body)
        db_type = data.get("type", "").lower()
        host    = data.get("host", "").strip()
        port    = int(data.get("port", 0) or 0)
        db_name = data.get("db", "").strip()
        schema  = data.get("schema", "").strip()
        user    = data.get("user", "").strip()
        password= data.get("pass", "")
        jdbc    = data.get("jdbc", "").strip()

        if not host or not user:
            return JsonResponse({"success": False, "error": "Host and username are required."})

        conn = None

        # ── MySQL ──────────────────────────────────────────────────────────
        if db_type == "mysql":
            try:
                conn = pymysql.connect(
                    host=host, port=port or 3306,
                    user=user, password=password,
                    database=db_name or None,
                    connect_timeout=5,
                )
                cur = conn.cursor()
                cur.execute("SELECT VERSION()")
                version = cur.fetchone()[0]
                return JsonResponse({"success": True, "message": f"MySQL {version}"})
            except ImportError:
                return JsonResponse({"success": False, "error": "pymysql not installed. Run: pip install pymysql"})

        # ── PostgreSQL ─────────────────────────────────────────────────────
        elif db_type == "postgres":
            try:
                import psycopg2
                conn = psycopg2.connect(
                    host=host, port=port or 5432,
                    user=user, password=password,
                    dbname=db_name or "postgres",
                    connect_timeout=5,
                    options=f"-c search_path={schema}" if schema else "",
                )
                cur = conn.cursor()
                cur.execute("SELECT version()")
                version = cur.fetchone()[0]
                return JsonResponse({"success": True, "message": version[:60]})
            except ImportError:
                return JsonResponse({"success": False, "error": "psycopg2 not installed. Run: pip install psycopg2-binary"})

        # ── Oracle ─────────────────────────────────────────────────────────
        elif db_type == "oracle":
            try:
                import oracledb
                dsn = jdbc or oracledb.makedsn(host, port or 1521, service_name=db_name)
                conn = oracledb.connect(user=user, password=password, dsn=dsn, timeout=5)
                cur  = conn.cursor()
                cur.execute("SELECT * FROM v$version WHERE ROWNUM=1")
                version = cur.fetchone()[0]
                return JsonResponse({"success": True, "message": version[:60]})
            except ImportError:
                return JsonResponse({"success": False, "error": "oracledb not installed. Run: pip install oracledb"})

        # ── SQL Server ─────────────────────────────────────────────────────
        elif db_type == "mssql":
            try:
                import pyodbc
                conn_str = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={host},{port or 1433};"
                    f"DATABASE={db_name};"
                    f"UID={user};PWD={password};"
                    f"Connection Timeout=5;"
                )
                conn = pyodbc.connect(conn_str, timeout=5)
                cur  = conn.cursor()
                cur.execute("SELECT @@VERSION")
                version = cur.fetchone()[0]
                return JsonResponse({"success": True, "message": version[:60]})
            except ImportError:
                return JsonResponse({"success": False, "error": "pyodbc not installed. Run: pip install pyodbc"})

        # ── Generic JDBC / Other ───────────────────────────────────────────
        else:
            # Try SQLAlchemy as a generic fallback
            try:
                from sqlalchemy import create_engine, text
                url = jdbc or f"postgresql+psycopg2://{user}:{password}@{host}:{port or 5432}/{db_name}"
                engine = create_engine(url, connect_args={"connect_timeout": 5})
                with engine.connect() as connection:
                    result = connection.execute(text("SELECT 1"))
                return JsonResponse({"success": True, "message": "Connected via SQLAlchemy"})
            except ImportError:
                return JsonResponse({"success": False, "error": "sqlalchemy not installed. Run: pip install sqlalchemy"})

    except Exception as e:
        return JsonResponse({"success": False, "error": str(e)})

    finally:
        try:
            if conn: conn.close()
        except Exception:
            pass


@login_required(login_url='/')
def table_builder_execute_sql(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            sql = data.get("sql", "")

            if not sql:
                return JsonResponse({"error": "No SQL provided"}, status=400)

            # Split multiple SQL statements
            statements = [s.strip() for s in sql.split(";") if s.strip()]
            conn = psycopg2.connect(**settings.DB_CONFIG)
            conn.autocommit = True
            with conn.cursor() as cursor:
                for stmt in statements:
                    cursor.execute(stmt)

            return JsonResponse({"status": "success"})

        except Exception as e:
            return JsonResponse({"error": str(e)}, status=500)
        

def login_view(request):
    # Redirect if already logged in
    if request.user.is_authenticated:
        return redirect('dashboard')

    form = AuthenticationForm(request, data=request.POST or None)

    if request.method == 'POST':
        if form.is_valid():
            user = form.get_user()
            login(request, user)

            # Handle "remember me" — session expires on browser close if unchecked
            if not request.POST.get('remember_me'):
                request.session.set_expiry(0)
            else:
                request.session.set_expiry(60 * 60 * 24 * 30)  # 30 days

            messages.success(request, f'Welcome back, {user.username}!')
            next_url = request.POST.get('next') or request.GET.get('next') or 'dashboard'
            return redirect(next_url)
        else:
            messages.error(request, 'Invalid username or password.')

    return render(request, 'login.html', {'form': form, 'next': request.GET.get('next', '')})

@login_required(login_url='/')
def logout_view(request):
    logout(request)
    messages.success(request, 'You have been signed out.')
    return redirect('pentaho_login')

@login_required(login_url='/')
@csrf_exempt
@require_http_methods(["POST"])
def generate_sql_preview(request):
    """
    Generate SQL preview using Claude AI from uploaded steps file.
    """
    if "file" not in request.FILES:
        return JsonResponse({"error": "No file uploaded"}, status=400)

    uploaded_file = request.FILES["file"]

    try:
        instructions = uploaded_file.read().decode("utf-8")
    except UnicodeDecodeError:
        return JsonResponse({"error": "File must be a UTF-8 text file"}, status=400)

    if not instructions.strip():
        return JsonResponse({"error": "Instruction file is empty"}, status=400)

    if not settings.ANTHROPIC_API_KEY:
        return JsonResponse({"error": "Claude API key not configured"}, status=500)
    

    user_prompt = f"""
    Convert the following steps file into a complete SQL script.
    Apply smart column type inference — do NOT make every column VARCHAR(255).
    Use INT for IDs, DECIMAL for scores/amounts, VARCHAR(20) for phone numbers,
    DATE/DATETIME for date fields, TEXT for descriptions, etc.
    Mark the primary key column appropriately.

    Steps file content:
    {instructions}
    """

    settings_obj = UserApiSettings.objects.last()

    model = settings_obj.model
    max_tokens = settings_obj.max_tokens
    timeout = settings_obj.timeout
    api_key = settings_obj.api_key
    
    # ── Define request params once ──────────────────────────────────────────
    url = "https://api.anthropic.com/v1/messages"
    headers = {
        "x-api-key":         api_key,
        "anthropic-version": "2023-06-01",
        "Content-Type":      "application/json",
    }
    payload = {
        "model":      model,
        "max_tokens": max_tokens, #2000
        "system":     SYSTEM_PROMPT_SQL_PRIVIEW,
        "messages":   [{"role": "user", "content": user_prompt}],
    }

    t0 = int(time.time() * 1000)

    try:
        # ── Single API call ──────────────────────────────────────────────────
        response = requests.post(url, headers=headers, json=payload, timeout=timeout, verify=False) #timeout : 30
        if response.status_code != 200:
            GenerationLog.objects.create(
                filename=uploaded_file.name or "generated_sql.sql",
                file_type="sql",
                status="error",
                error_message=f"API Error {response.status_code}: {response.text[:500]}",
                duration_ms=int(time.time() * 1000) - t0,
            )
            return JsonResponse(
                {"error": f"API Error {response.status_code}: {response.text}"},
                status=500,
            )

        body = response.json()

        # ── Extract SQL text from Claude response ────────────────────────────
        sql = body.get("content", [{}])[0].get("text", "").strip()

        # Strip markdown code fences if present (```sql ... ``` or ``` ... ```)
        if sql.startswith("```"):
            sql = sql.split("\n", 1)[1] if "\n" in sql else ""
        if sql.endswith("```"):
            sql = sql.rsplit("```", 1)[0]
        sql = sql.strip()

        # ── Log success ──────────────────────────────────────────────────────
        usage = body.get("usage", {})
        GenerationLog.objects.create(
            filename=uploaded_file.name or "generated_sql.sql",
            file_type="sql",
            status="ok",
            input_tokens=usage.get("input_tokens", 0),
            output_tokens=usage.get("output_tokens", 0),
            duration_ms=int(time.time() * 1000) - t0,
        )

        return JsonResponse({
            "sql":          sql,
            "instructions": instructions,
        })

    except requests.exceptions.Timeout:
        GenerationLog.objects.create(
            filename=uploaded_file.name or "generated_sql.sql",
            file_type="sql",
            status="error",
            error_message="Request timed out after 30 seconds",
            duration_ms=int(time.time() * 1000) - t0,
        )
        return JsonResponse({"error": "Request timed out. Please try again."}, status=504)

    except Exception as e:
        GenerationLog.objects.create(
            filename=uploaded_file.name or "generated_sql.sql",
            file_type="sql",
            status="error",
            error_message=str(e)[:500],
            duration_ms=int(time.time() * 1000) - t0,
        )
        return JsonResponse({"error": f"Unexpected error: {str(e)}"}, status=500)


# ── Save files from ZIP response into GeneratedFile model ──────────────────
def save_generated_files(zip_blob, source="generator", log=None):
    """
    Call this after any generation endpoint returns a ZIP.
    zip_blob = bytes of the zip file
    Returns list of GeneratedFile instances created.
    """
    saved = []
    with zipfile.ZipFile(io.BytesIO(zip_blob)) as z:
        for fname in z.namelist():
            content  = z.read(fname)
            ext      = fname.rsplit('.', 1)[-1].lower()
            basename = fname.rsplit('.', 1)[0]

            gf = GeneratedFile(
                name      = basename,
                file_type = ext,
                size_bytes= len(content),
                source    = source,
                log       = log,
            )
            gf.file.save(fname, ContentFile(content), save=True)
            saved.append(gf)
    return saved


# ── List all stored files (paginated) ──────────────────────────────────────
@login_required(login_url='/')
@require_http_methods(["GET"])
def downloads_list(request):
    ftype  = request.GET.get('type', 'all')
    source = request.GET.get('source', 'all')
    q      = request.GET.get('q', '').strip()
    page   = max(1, int(request.GET.get('page', 1)))
    limit  = int(request.GET.get('limit', 20))

    qs = GeneratedFile.objects.all()
    if ftype  != 'all': qs = qs.filter(file_type=ftype)
    if source != 'all': qs = qs.filter(source=source)
    if q:               qs = qs.filter(name__icontains=q)

    total       = qs.count()
    total_pages = max(1, (total + limit - 1) // limit)
    rows        = qs[(page-1)*limit : page*limit]

    return JsonResponse({
        'total':       total,
        'total_pages': total_pages,
        'page':        page,
        'limit':       limit,
        'rows': [{
            'id':       f.id,
            'name':     f.name,
            'type':     f.file_type,
            'size':     f.size_display(),
            'source':   f.source,
            'time_ms':  int(f.created_at.timestamp() * 1000),
            'url':      request.build_absolute_uri(f.file.url),
        } for f in rows],
    })


# ── Download single file ────────────────────────────────────────────────────
@login_required(login_url='/')
@require_http_methods(["GET"])
def download_file(request, file_id):
    try:
        gf = GeneratedFile.objects.get(pk=file_id)
    except GeneratedFile.DoesNotExist:
        return JsonResponse({'error': 'File not found'}, status=404)

    response = FileResponse(
        gf.file.open('rb'),
        as_attachment=True,
        filename=gf.filename(),
    )
    return response


# ── Download multiple files as ZIP ─────────────────────────────────────────
@login_required(login_url='/')
@require_http_methods(["GET"])
def download_zip(request):
    ids = request.GET.get('ids', '')
    if not ids:
        return JsonResponse({'error': 'No file IDs provided'}, status=400)

    id_list = [int(i) for i in ids.split(',') if i.strip().isdigit()]
    files   = GeneratedFile.objects.filter(pk__in=id_list)

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as z:
        for f in files:
            z.writestr(f.filename(), f.file.read())
    buf.seek(0)

    response = HttpResponse(buf.read(), content_type='application/zip')
    response['Content-Disposition'] = 'attachment; filename="pentaho_downloads.zip"'
    return response


# ── Delete a stored file ────────────────────────────────────────────────────
@login_required(login_url='/')
@csrf_exempt
@require_http_methods(["DELETE", "POST"])
def delete_file(request, file_id):
    try:
        gf = GeneratedFile.objects.get(pk=file_id)
        gf.file.delete(save=False)   # remove from disk
        gf.delete()
        return JsonResponse({'deleted': True})
    except GeneratedFile.DoesNotExist:
        return JsonResponse({'error': 'File not found'}, status=404)


# ── Delete all files ────────────────────────────────────────────────────────
@login_required(login_url='/')
@csrf_exempt
@require_http_methods(["POST"])
def delete_all_files(request):
    files = GeneratedFile.objects.all()
    count = files.count()
    for f in files:
        f.file.delete(save=False)
    files.delete()
    return JsonResponse({'deleted': count})


# ── Downloads stats (for badge counts) ─────────────────────────────────────
@login_required(login_url='/')
@require_http_methods(["GET"])
def downloads_stats(request):
    return JsonResponse({
        'total': GeneratedFile.objects.count(),
        'ktr':   GeneratedFile.objects.filter(file_type='ktr').count(),
        'kjb':   GeneratedFile.objects.filter(file_type='kjb').count(),
        'sql':   GeneratedFile.objects.filter(file_type='sql').count(),
    })


@login_required(login_url='/')
@require_POST
def save_api_settings(request):
    try:
        data = json.loads(request.body)
        api_key    = data.get("apiKey", "").strip()
        model      = data.get("model")
        max_tokens = int(data.get("maxTokens", 16000))
        timeout    = int(data.get("timeout", 180))

        obj, _ = UserApiSettings.objects.update_or_create(
            user=request.user,
            defaults={
                "api_key": api_key,
                "model": model,
                "max_tokens": max_tokens,
                "timeout": timeout,
            }
        )

        return JsonResponse({"status": "ok"})
    except Exception as e:
        return JsonResponse({"status": "error", "msg": str(e)}, status=400)
    

@login_required(login_url='/')
@require_GET
def get_api_settings(request):
    try:
        obj = UserApiSettings.objects.filter(user=request.user).first()
        if not obj:
            return JsonResponse({"status": "ok", "apiKey": "", "model": "", "maxTokens": 16000, "timeout": 180})
        return JsonResponse({
            "status": "ok",
            "apiKey": obj.api_key,
            "model": obj.model,
            "maxTokens": obj.max_tokens,
            "timeout": obj.timeout
        })
    except Exception as e:
        return JsonResponse({"status": "error", "msg": str(e)}, status=400)

  
@login_required(login_url='/')
@require_POST
def test_api_key(request):
    try:
        data = json.loads(request.body)
        api_key = data.get("apiKey", "").strip()
        model = data.get("model", "").strip()
        max_tokens = data.get("max_tokens", 1000)
        timeout = data.get("timeout", 60)

        if not api_key:
            return JsonResponse({"status": "error", "msg": "API key is required"}, status=400)

        ANTHROPIC_TEST_URL = "https://api.anthropic.com/v1/messages"

        # ✅ Correct headers for Anthropic — x-api-key, NOT Authorization: Bearer
        headers = {
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }

        # ✅ Must be POST with a minimal valid body — GET returns 405
        payload = {
            "model": model,
            "max_tokens": 1,
            "messages": [{"role": "user", "content": "hi"}]
        }

        resp = requests.post(
            ANTHROPIC_TEST_URL,
            headers=headers,
            json=payload,
            timeout=10,
            verify=False
        )

        if resp.status_code in (200, 400):
            # 400 means the request was understood (key is valid) but params were bad
            return JsonResponse({"status": "ok"})
        elif resp.status_code == 401:
            return JsonResponse({"status": "error", "msg": "Invalid API key"}, status=401)
        elif resp.status_code == 429:
            return JsonResponse({"status": "error", "msg": "Rate limited — but key is valid"}, status=200)
        else:
            return JsonResponse({"status": "error", "msg": f"HTTP {resp.status_code}"}, status=resp.status_code)

    except requests.exceptions.ConnectionError:
        return JsonResponse({"status": "error", "msg": "Could not reach Anthropic API"}, status=503)
    except requests.exceptions.Timeout:
        return JsonResponse({"status": "error", "msg": "Request timed out"}, status=504)
    except requests.exceptions.RequestException as e:
        return JsonResponse({"status": "error", "msg": str(e)}, status=500)
    
def config_api(request):
    ANTHROPIC_API_KEY = config("ANTHROPIC_API_KEY")
    ANTHROPIC_MODEL   = config("ANTHROPIC_MODEL")
    ANTHROPIC_maxTokens   = config("ANTHROPIC_maxTokens")
    ANTHROPIC_timeout   = config("ANTHROPIC_timeout")

    return JsonResponse({
        "api_key": ANTHROPIC_API_KEY or "", "model": ANTHROPIC_MODEL, "max_tokens": ANTHROPIC_maxTokens, "timeout": ANTHROPIC_timeout
    })

@csrf_exempt
def save_preferences(request):
    if request.method == "POST":
        data = json.loads(request.body)

        # get first record or create one
        prefs, created = UserPreference.objects.get_or_create(id=1)

        prefs.refresh = data.get("refresh", 60)
        prefs.toast = data.get("toast", True)
        prefs.auto_download = data.get("auto_download", True)
        prefs.debug = data.get("debug", False)
        prefs.history_limit = data.get("history_limit", 50)
        prefs.folder_name = data.get("folder_name", "")
        prefs.save()

        return JsonResponse({"status": "ok"})


def get_preferences(request):
    try:
        prefs = UserPreference.objects.get(id=1)

        return JsonResponse({
            "refresh": prefs.refresh,
            "toast": prefs.toast,
            "auto_download": prefs.auto_download,
            "debug": prefs.debug,
            "history_limit": prefs.history_limit,
            "folder_name": prefs.folder_name,
        })
    except UserPreference.DoesNotExist:
        return JsonResponse({})

q = queue.Queue()

# Initialize model once
# model = vosk.Model("C:/Users/prabhakaranp/Documents/prabhakaran/ktr_generator/transformer/vosk-model-small-en-us-0.15")
# rec = vosk.KaldiRecognizer(model, 16000)

# Callback only puts audio in queue
def callback(indata, frames, time, status):
    if status:
        print(status)
    q.put(bytes(indata))

    # Start microphone stream
    with sd.RawInputStream(samplerate=16000, blocksize=8000, dtype='int16',
                        channels=1, callback=callback):
        print("Speak now...")
        while True:
            data = q.get()
            if rec.AcceptWaveform(data):
                result = json.loads(rec.Result())["text"]
                print(result)

def console(request):
    tables = []
    query = ""
    columns = []
    rows = []
    error = None

    # ✅ ALWAYS define it first
    active_table = None

    # Get tables
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT name 
            FROM sqlite_master 
            WHERE type='table';
        """)
        tables = [row[0] for row in cursor.fetchall()]

    # Run query
    if request.method == "POST":
        query = request.POST.get("query")

        # extract table name safely
        match = re.search(r"from\s+([a-zA-Z0-9_]+)", query, re.IGNORECASE)
        if match:
            active_table = match.group(1)

        try:
            with connection.cursor() as cursor:
                cursor.execute(query)

                if cursor.description:
                    columns = [col[0] for col in cursor.description]
                    rows = cursor.fetchall()

        except Exception as e:
            error = str(e)

    return render(request, "db_view.html", {
        "tables": tables,
        "query": query,
        "columns": columns,
        "rows": rows,
        "error": error,
        "active_table": active_table
    })
