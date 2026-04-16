"""
views_talend_project.py  ─ FIXED VERSION
========================================
Generates a Talend project ZIP that matches the EXACT GAININSIGHTS structure:

GAININSIGHTS/
├── talend.project
├── .settings/
│   ├── migration_task.index
│   ├── project.settings
│   ├── relationship.index
│   └── links/
│       └── *.link  (one per job)
├── context/                         ← FILES ARE FLAT (no subfolders)
│   ├── accel_date_parameters_0.1.item
│   ├── accel_date_parameters_0.1    ← NO .properties extension
│   └── ...
├── metadata/
│   ├── connections/                 ← FLAT files
│   │   ├── ConnectionName_0.1.item
│   │   └── ConnectionName_0.1
│   ├── fileDelimited/
│   │   ├── File_0.1.item
│   │   └── File_0.1
│   └── fileExcel/
│       ├── ExcelName_0.1.item
│       └── ExcelName_0.1
├── code/routines/                   ← FLAT files
│   ├── endecryption_0.1.item
│   └── endecryption_0.1
├── sqlPatterns/
│   ├── Generic/system/   ├── Hive/system/   ├── MySQL/system/
│   ├── Oracle/system/    ├── Teradata/system/ └── ...
└── process/
    ├── JobName_0.1.item             ← root-level jobs
    ├── JobName_0.1
    ├── JobName_0.1.screenshot
    └── SubFolder/                   ← grouped jobs
        ├── JobName_0.1.item
        └── JobName_0.1
"""

import io
import re
import time
import uuid
import zipfile

import requests
import urllib3
from django.contrib.auth.decorators import login_required
from django.core.files.base import ContentFile
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render, redirect
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.forms import AuthenticationForm
from .models import GenerationLog, GeneratedFile, UserApiSettings, UserPreference
from django.contrib import messages
from django.utils import timezone
from datetime import timedelta
from django.db.models import Avg, Count, Q, Sum

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def talend_login(request):
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
            next_url = request.POST.get('next') or request.GET.get('next') or 'talend_dashboard'
            return redirect(next_url)
        else:
            messages.error(request, 'Invalid username or password.')

    return render(request, 'login_talend.html', {'form': form, 'next': request.GET.get('next', '')})


@login_required(login_url='/')
def talend_logout(request):
    logout(request)
    messages.success(request, 'You have been signed out.')
    return redirect('talend_login')


@login_required(login_url='/')
def talend_dashboard(request):
    settings_obj = UserApiSettings.objects.last()

    model = settings_obj.model
    max_tokens = settings_obj.max_tokens
    timeout = settings_obj.timeout
    api_key = settings_obj.api_key

    return render(request, "dashboard_Talend.html", {
       "active_page": "dashboard", "api_key": api_key or "", "model": model, "max_tokens": max_tokens, "timeout": timeout
    })

# ─────────────────────────────────────────────────────────────────────────────
# PROJECT CONSTANTS  (from your real talend.project)
# ─────────────────────────────────────────────────────────────────────────────

ROOT = "GAININSIGHTS"

TALEND_PROJECT_XML = '''\
<?xml version="1.0" encoding="UTF-8"?>
<xmi:XMI xmi:version="2.0" xmlns:xmi="http://www.omg.org/XMI" xmlns:TalendProperties="http://www.talend.org/properties">
  <TalendProperties:Project xmi:id="_Rt5XcSZtEeyMZIJ95Km03Q" label="GainInsights" description="All The projects done by GainInsights" language="java" technicalLabel="GAININSIGHTS" author="_Rt5XciZtEeyMZIJ95Km03Q" productVersion="Talend Cloud Data Management Platform-8.0.1.20250704_1436-patch" url="{&quot;commitLogPattern&quot;:&quot;{0}&quot;,&quot;location&quot;:&quot;https://kai-gitlab.us.kioxia.com/ssd-bu/talend-integration.git&quot;,&quot;settings&quot;:&quot;000&quot;,&quot;storage&quot;:&quot;git&quot;}" type="DQ" itemsRelationVersion="1.3" bigData="false">
    <migrationTask xmi:id="_SCl1ECZtEeyMZIJ95Km03Q" id="org.talend.repository.model.migration.CheckProductVersionMigrationTask" breaks="7.1.0" version="7.1.1"/>
  </TalendProperties:Project>
  <TalendProperties:User xmi:id="_Rt5XciZtEeyMZIJ95Km03Q" login="guynadler@taec.toshiba.com" firstName="guynadler" lastName="removed" type="DQ"/>
  <TalendProperties:User xmi:id="_WTAhYCZtEeyMZIJ95Km03Q" login="talend_admin@taec.toshiba.com" firstName="Shruthi K" lastName="Kamath" type="DQ"/>
  <TalendProperties:User xmi:id="_AgtucK6QEeywmdylt3BrMw" login="guynadler@kioxia.com" firstName="Krutika" lastName="Pai" type="DQ"/>
  <TalendProperties:User xmi:id="_Agtuca6QEeywmdylt3BrMw" login="talend_admin@kioxia.com" firstName="Gain" lastName="Insight" type="DQ"/>
  <TalendProperties:User xmi:id="_LAGZ8OGAEe66oM_fiVJgmg" login="aaryan.gupta@kioxia.com" firstName="Aaryan" lastName="Gupta" type="DQ"/>
  <TalendProperties:User xmi:id="_kcdbGEpzEfCX6shmM4cwVg" login="matthew.yano@kioxia.com" firstName="Talend" lastName="Admin" type="DQ"/>
  <TalendProperties:User xmi:id="_xc7f0HfEEfCOQ9lkil8AFQ" login="krutika.pai@kioxia.com" firstName="Krutika" lastName="Pai" type="DQ"/>
  <TalendProperties:User xmi:id="_JuWKALSGEfC9Cv1zCAVKnw" login="talend.adminetl@kioxia.com" firstName="Talend" lastName="Admin" type="DQ"/>
</xmi:XMI>
'''

# ── .settings files ──────────────────────────────────────────────────────────

PROJECT_SETTINGS = '''\
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<settings>
  <setting id="org.talend.core.runtime.services.IProxyService.proxyHost" value=""/>
  <setting id="org.talend.core.runtime.services.IProxyService.proxyPort" value=""/>
  <setting id="org.talend.core.runtime.services.IProxyService.proxyUser" value=""/>
  <setting id="org.talend.core.runtime.services.IProxyService.proxyPassword" value=""/>
  <setting id="org.talend.core.runtime.services.IProxyService.nonProxyHosts" value=""/>
  <setting id="repositoryObjectType" value="process"/>
  <setting id="statAndLogsSettings.implicitTContextLoad" value="false"/>
  <setting id="statAndLogsSettings.multiThread" value="false"/>
</settings>
'''

# migration_task.index and relationship.index are binary-like XML in real Talend
# We write minimal valid versions
MIGRATION_TASK_INDEX = '''\
<?xml version="1.0" encoding="UTF-8"?>
<index>
</index>
'''

RELATIONSHIP_INDEX = '''\
<?xml version="1.0" encoding="UTF-8"?>
<index>
</index>
'''

# ── SQL Pattern item template ─────────────────────────────────────────────────

def _sql_pattern_item(name: str, db_type: str, sql_body: str) -> str:
    xmi_id = "_" + uuid.uuid4().hex[:20]
    return f'''\
<?xml version="1.0" encoding="UTF-8"?>
<xmi:XMI xmi:version="2.0" xmlns:xmi="http://www.omg.org/XMI"
    xmlns:TalendProperties="http://www.talend.org/properties"
    xmlns:SQLPattern="http://www.talend.org/sqlpattern">
  <TalendProperties:Property xmi:id="{xmi_id}" label="{name}" version="0.1" statusCode="PROD"
    displayName="{name}" purpose="SQL Pattern for {db_type}">
    <item xsi:type="SQLPattern:SQLPatternItem" xmi:id="_{uuid.uuid4().hex[:20]}"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
      <pattern name="{name}" dbms="{db_type}">
        <entries>{sql_body}</entries>
      </pattern>
    </item>
  </TalendProperties:Property>
</xmi:XMI>
'''

def _sql_pattern_props(name: str) -> str:
    return f'''\
<?xml version="1.0" encoding="UTF-8"?>
<xmi:XMI xmi:version="2.0" xmlns:xmi="http://www.omg.org/XMI"
    xmlns:TalendProperties="http://www.talend.org/properties">
  <TalendProperties:Property xmi:id="_{uuid.uuid4().hex[:20]}"
    label="{name}" version="0.1" statusCode="PROD"/>
</xmi:XMI>
'''

# ── All SQL patterns matching real GAININSIGHTS structure ─────────────────────

SQL_PATTERNS = {
    "Generic": {
        "system": [
            ("Aggregate",        "SELECT {GROUP_BY_COLUMNS}, {AGGREGATE_FUNCTIONS} FROM {SOURCE_TABLE} GROUP BY {GROUP_BY_COLUMNS}"),
            ("Commit",           "COMMIT"),
            ("DropSourceTable",  "DROP TABLE IF EXISTS {SOURCE_TABLE}"),
            ("DropTargetTable",  "DROP TABLE IF EXISTS {TARGET_TABLE}"),
            ("FilterColumns",    "SELECT {COLUMNS} FROM {SOURCE_TABLE}"),
            ("FilterRow",        "SELECT * FROM {SOURCE_TABLE} WHERE {CONDITION}"),
            ("MergeInsert",      "INSERT INTO {TARGET_TABLE} ({COLUMNS}) SELECT {COLUMNS} FROM {SOURCE_TABLE}"),
            ("MergeUpdate",      "UPDATE {TARGET_TABLE} SET {SET_CLAUSE} WHERE {CONDITION}"),
            ("Rollback",         "ROLLBACK"),
        ]
    },
    "Hive": {
        "system": [
            ("HiveAggregate",           "SELECT {GROUP_BY_COLUMNS}, {AGGREGATE_FUNCTIONS} FROM {SOURCE_TABLE} GROUP BY {GROUP_BY_COLUMNS}"),
            ("HiveCreateSourceTable",   "CREATE TABLE IF NOT EXISTS {SOURCE_TABLE} ({COLUMN_DEFINITIONS}) STORED AS ORC"),
            ("HiveCreateTargetTable",   "CREATE TABLE IF NOT EXISTS {TARGET_TABLE} ({COLUMN_DEFINITIONS}) STORED AS ORC"),
            ("HiveDropSourceTable",     "DROP TABLE IF EXISTS {SOURCE_TABLE}"),
            ("HiveDropTargetTable",     "DROP TABLE IF EXISTS {TARGET_TABLE}"),
            ("HiveFilterColumns",       "SELECT {COLUMNS} FROM {SOURCE_TABLE}"),
            ("HiveFilterRow",           "SELECT * FROM {SOURCE_TABLE} WHERE {CONDITION}"),
        ]
    },
    "MySQL": {
        "system": [
            ("MySQLAggregate",          "SELECT {GROUP_BY_COLUMNS}, {AGGREGATE_FUNCTIONS} FROM {SOURCE_TABLE} GROUP BY {GROUP_BY_COLUMNS}"),
            ("MySQLCreateSourceTable",  "CREATE TABLE IF NOT EXISTS {SOURCE_TABLE} ({COLUMN_DEFINITIONS}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"),
            ("MySQLCreateTargetTable",  "CREATE TABLE IF NOT EXISTS {TARGET_TABLE} ({COLUMN_DEFINITIONS}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"),
            ("MySQLDropSourceTable",    "DROP TABLE IF EXISTS {SOURCE_TABLE}"),
            ("MySQLDropTargetTable",    "DROP TABLE IF EXISTS {TARGET_TABLE}"),
            ("MySQLFilterColumns",      "SELECT {COLUMNS} FROM {SOURCE_TABLE}"),
            ("MySQLFilterRow",          "SELECT * FROM {SOURCE_TABLE} WHERE {CONDITION}"),
        ]
    },
    "Netezza": {
        "system": [
            ("NetezzaAggregate",          "SELECT {GROUP_BY_COLUMNS}, {AGGREGATE_FUNCTIONS} FROM {SOURCE_TABLE} GROUP BY {GROUP_BY_COLUMNS}"),
            ("NetezzaCreateSourceTable",  "CREATE TABLE {SOURCE_TABLE} ({COLUMN_DEFINITIONS}) DISTRIBUTE ON RANDOM"),
            ("NetezzaCreateTargetTable",  "CREATE TABLE {TARGET_TABLE} ({COLUMN_DEFINITIONS}) DISTRIBUTE ON RANDOM"),
            ("NetezzaDropSourceTable",    "DROP TABLE {SOURCE_TABLE} IF EXISTS"),
            ("NetezzaDropTargetTable",    "DROP TABLE {TARGET_TABLE} IF EXISTS"),
            ("NetezzaFilterColumns",      "SELECT {COLUMNS} FROM {SOURCE_TABLE}"),
            ("NetezzaFilterRow",          "SELECT * FROM {SOURCE_TABLE} WHERE {CONDITION}"),
        ]
    },
    "Oracle": {
        "system": [
            ("OracleAggregate",          "SELECT {GROUP_BY_COLUMNS}, {AGGREGATE_FUNCTIONS} FROM {SOURCE_TABLE} GROUP BY {GROUP_BY_COLUMNS}"),
            ("OracleCreateSourceTable",  "CREATE TABLE {SOURCE_TABLE} ({COLUMN_DEFINITIONS})"),
            ("OracleCreateTargetTable",  "CREATE TABLE {TARGET_TABLE} ({COLUMN_DEFINITIONS})"),
            ("OracleDropSourceTable",    "DROP TABLE {SOURCE_TABLE} PURGE"),
            ("OracleDropTargetTable",    "DROP TABLE {TARGET_TABLE} PURGE"),
            ("OracleFilterColumns",      "SELECT {COLUMNS} FROM {SOURCE_TABLE}"),
            ("OracleFilterRow",          "SELECT * FROM {SOURCE_TABLE} WHERE {CONDITION}"),
            ("OracleMerge",              "MERGE INTO {TARGET_TABLE} t USING {SOURCE_TABLE} s ON ({JOIN_CONDITION}) WHEN MATCHED THEN UPDATE SET {SET_CLAUSE} WHEN NOT MATCHED THEN INSERT ({COLUMNS}) VALUES ({VALUES})"),
        ]
    },
    "ParAccel": {
        "system": [
            ("ParAccelAggregate",        "SELECT {GROUP_BY_COLUMNS}, {AGGREGATE_FUNCTIONS} FROM {SOURCE_TABLE} GROUP BY {GROUP_BY_COLUMNS}"),
            ("ParAccelCommit",           "COMMIT"),
            ("ParAccelDropSourceTable",  "DROP TABLE IF EXISTS {SOURCE_TABLE}"),
            ("ParAccelDropTargetTable",  "DROP TABLE IF EXISTS {TARGET_TABLE}"),
            ("ParAccelFilterColumns",    "SELECT {COLUMNS} FROM {SOURCE_TABLE}"),
            ("ParAccelFilterRow",        "SELECT * FROM {SOURCE_TABLE} WHERE {CONDITION}"),
            ("ParAccelRollback",         "ROLLBACK"),
        ]
    },
    "Snowflake": {
        "system": [
            ("SnowflakeCreateSourceTable", "CREATE TABLE IF NOT EXISTS {SOURCE_TABLE} ({COLUMN_DEFINITIONS})"),
            ("SnowflakeCreateTargetTable", "CREATE TABLE IF NOT EXISTS {TARGET_TABLE} ({COLUMN_DEFINITIONS})"),
            ("DropSourceTable",            "DROP TABLE IF EXISTS {SOURCE_TABLE}"),
            ("SnowflakeDropSourceTable",   "DROP TABLE IF EXISTS {SOURCE_TABLE}"),
            ("SnowflakeDropTargetTable",   "DROP TABLE IF EXISTS {TARGET_TABLE}"),
            ("SnowflakeMerge",             "MERGE INTO {TARGET_TABLE} t USING {SOURCE_TABLE} s ON t.{KEY} = s.{KEY} WHEN MATCHED THEN UPDATE SET {SET_CLAUSE} WHEN NOT MATCHED THEN INSERT ({COLUMNS}) VALUES ({VALUES})"),
        ]
    },
    "Vertica": {
        "system": [
            ("VerticaMergeInsert", "INSERT INTO {TARGET_TABLE} ({COLUMNS}) SELECT {COLUMNS} FROM {SOURCE_TABLE} WHERE NOT EXISTS (SELECT 1 FROM {TARGET_TABLE} WHERE {CONDITION})"),
            ("VerticaMergeUpdate", "UPDATE {TARGET_TABLE} SET {SET_CLAUSE} FROM {SOURCE_TABLE} WHERE {JOIN_CONDITION}"),
        ]
    },
    "DeltaLake": {
        "system": [
            ("DeltaLakeAggregate",         "SELECT {GROUP_BY_COLUMNS}, {AGGREGATE_FUNCTIONS} FROM {SOURCE_TABLE} GROUP BY {GROUP_BY_COLUMNS}"),
            ("DropSourceTable",            "DROP TABLE IF EXISTS {SOURCE_TABLE}"),
            ("DeltaLakeCreateDeltaTable",  "CREATE TABLE IF NOT EXISTS {TARGET_TABLE} ({COLUMN_DEFINITIONS}) USING DELTA LOCATION '{LOCATION}'"),
            ("DeltaLakeCreateTable",       "CREATE TABLE IF NOT EXISTS {TARGET_TABLE} ({COLUMN_DEFINITIONS}) USING DELTA"),
            ("DeltaLakeDropTable",         "DROP TABLE IF EXISTS {TARGET_TABLE}"),
            ("DeltaLakeFilterColumns",     "SELECT {COLUMNS} FROM {SOURCE_TABLE}"),
            ("DeltaLakeFilterRows",        "SELECT * FROM {SOURCE_TABLE} WHERE {CONDITION}"),
            ("DeltaLakeMerge",             "MERGE INTO {TARGET_TABLE} t USING {SOURCE_TABLE} s ON t.{KEY} = s.{KEY} WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"),
        ]
    },
    "Teradata": {
        "system": [
            ("TeradataAggregate",           "SELECT {GROUP_BY_COLUMNS}, {AGGREGATE_FUNCTIONS} FROM {SOURCE_TABLE} GROUP BY {GROUP_BY_COLUMNS}"),
            ("TeradataColumnList",          "SELECT COLUMN_NAME FROM DBC.COLUMNS WHERE TABLENAME = '{TABLE_NAME}'"),
            ("TeradataCreateSourceTable",   "CREATE SET TABLE {SOURCE_TABLE} ({COLUMN_DEFINITIONS}) PRIMARY INDEX ({PRIMARY_INDEX})"),
            ("TeradataCreateTargetTable",   "CREATE SET TABLE {TARGET_TABLE} ({COLUMN_DEFINITIONS}) PRIMARY INDEX ({PRIMARY_INDEX})"),
            ("TeradataDropSourceTable",     "DROP TABLE {SOURCE_TABLE}"),
            ("TeradataDropTargetTable",     "DROP TABLE {TARGET_TABLE}"),
            ("TeradataFilterColumns",       "SELECT {COLUMNS} FROM {SOURCE_TABLE}"),
            ("TeradataFilterRow",           "SELECT * FROM {SOURCE_TABLE} WHERE {CONDITION}"),
            ("TeradataTableList",           "SELECT TABLENAME FROM DBC.TABLES WHERE DATABASENAME = '{DATABASE_NAME}'"),
        ]
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# XMI ID + FILE GENERATORS
# ─────────────────────────────────────────────────────────────────────────────

def _xid() -> str:
    return "_" + uuid.uuid4().hex[:20]


def _make_props_noext(label: str, item_type: str = "Generated", version: str = "0.1") -> str:
    """
    Properties sidecar file — NO file extension in Talend 8.
    Named like: JobName_0.1  (not JobName_0.1.properties)
    """
    return (
        f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<xmi:XMI xmi:version="2.0" xmlns:xmi="http://www.omg.org/XMI" '
        f'xmlns:TalendProperties="http://www.talend.org/properties">\n'
        f'  <TalendProperties:Property xmi:id="{_xid()}"\n'
        f'    label="{label}" version="{version}" statusCode="PROD"\n'
        f'    displayName="{label}" purpose="{item_type}"\n'
        f'    description="Generated by GainInsights AI"\n'
        f'    creationDate="20250407 17:45:00"\n'
        f'    modificationDate="20250407 17:45:00"\n'
        f'    author="_Rt5XciZtEeyMZIJ95Km03Q"/>\n'
        f'</xmi:XMI>\n'
    )


def _make_link_file(label: str) -> str:
    """Link file stored in .settings/links/  — named with xmi id pattern."""
    return (
        f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<xmi:XMI xmi:version="2.0" xmlns:xmi="http://www.omg.org/XMI" '
        f'xmlns:TalendProperties="http://www.talend.org/properties">\n'
        f'  <TalendProperties:ItemRelationShip xmi:id="{_xid()}"\n'
        f'    base="{label}" type="process"/>\n'
        f'</xmi:XMI>\n'
    )


def _make_screenshot() -> str:
    """Minimal screenshot file (empty PNG-like placeholder)."""
    return "<!-- screenshot placeholder -->\n"


# ─────────────────────────────────────────────────────────────────────────────
# ZIP BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def _build_talend_project_zip(parsed: dict) -> bytes:
    """
    Build the full Talend project ZIP with EXACT GAININSIGHTS structure.

    Key rules from real project screenshots:
    - context/ files are FLAT (no subfolders)
    - code/routines/ files are FLAT (no subfolders)
    - metadata/connections/ files are FLAT
    - Properties sidecar has NO extension (e.g.  JobName_0.1  not  JobName_0.1.properties)
    - process/ jobs can be at root or in a subfolder
    - .settings/ has migration_task.index, project.settings, relationship.index, links/
    - sqlPatterns/ has all DB engine folders with system/ and UserDefined/ subfolders
    """
    buf = io.BytesIO()

    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:

        # ── talend.project ────────────────────────────────────────────────
        zf.writestr(f"{ROOT}/talend.project", TALEND_PROJECT_XML)

        # ── .settings/ ────────────────────────────────────────────────────
        zf.writestr(f"{ROOT}/.settings/project.settings",       PROJECT_SETTINGS)
        zf.writestr(f"{ROOT}/.settings/migration_task.index",   MIGRATION_TASK_INDEX)
        zf.writestr(f"{ROOT}/.settings/relationship.index",     RELATIONSHIP_INDEX)

        # links — one per process job
        for proc in parsed.get("PROCESS", []):
            link_id = "_" + uuid.uuid4().hex[:22] + ".link"
            zf.writestr(
                f"{ROOT}/.settings/links/{link_id}",
                _make_link_file(proc["name"]),
            )

        # ── context/  FLAT — no subfolders ────────────────────────────────
        for ctx in parsed.get("CONTEXT", []):
            name    = ctx["name"]
            version = ctx.get("version", "0.1")
            base    = f"{ROOT}/context/{name}_{version}"
            zf.writestr(f"{base}.item", ctx["content"])
            zf.writestr(base,           _make_props_noext(name, "Context Group", version))

        # ── code/routines/  FLAT ──────────────────────────────────────────
        for rout in parsed.get("ROUTINE", []):
            name    = rout["name"]
            version = rout.get("version", "0.1")
            base    = f"{ROOT}/code/routines/{name}_{version}"
            zf.writestr(f"{base}.item", rout["content"])
            zf.writestr(base,           _make_props_noext(name, "Java Routine", version))

        # ── metadata/connections/  FLAT ───────────────────────────────────
        for conn in parsed.get("CONNECTION", []):
            name    = conn["name"]
            version = conn.get("version", "0.1")
            base    = f"{ROOT}/metadata/connections/{name}_{version}"
            zf.writestr(f"{base}.item", conn["content"])
            zf.writestr(base,           _make_props_noext(name, "DB Connection", version))

        # ── metadata/fileDelimited/ ───────────────────────────────────────
        for fd in parsed.get("FILE_DELIMITED", []):
            name    = fd["name"]
            version = fd.get("version", "0.1")
            base    = f"{ROOT}/metadata/fileDelimited/{name}_{version}"
            zf.writestr(f"{base}.item", fd["content"])
            zf.writestr(base,           _make_props_noext(name, "File Delimited", version))

        # ── metadata/fileExcel/ ───────────────────────────────────────────
        for fx in parsed.get("FILE_EXCEL", []):
            name    = fx["name"]
            version = fx.get("version", "0.1")
            base    = f"{ROOT}/metadata/fileExcel/{name}_{version}"
            zf.writestr(f"{base}.item", fx["content"])
            zf.writestr(base,           _make_props_noext(name, "File Excel", version))

        # ── process/ ──────────────────────────────────────────────────────
        # Jobs can be at root of process/ OR inside a subfolder
        # Subfolder is taken from proc["folder"] if set, else root
        for proc in parsed.get("PROCESS", []):
            name    = proc["name"]
            version = proc.get("version", "0.1")
            folder  = proc.get("folder", "")   # e.g. "PROD_JOBS" or ""
            if folder:
                base = f"{ROOT}/process/{folder}/{name}_{version}"
            else:
                base = f"{ROOT}/process/{name}_{version}"
            zf.writestr(f"{base}.item",       proc["content"])
            zf.writestr(base,                 _make_props_noext(name, "ETL Process Job", version))
            zf.writestr(f"{base}.screenshot", _make_screenshot())

        # ── sqlPatterns/ ──────────────────────────────────────────────────
        for db_name, sections in SQL_PATTERNS.items():
            # UserDefined/ always present (empty)
            zf.writestr(f"{ROOT}/sqlPatterns/{db_name}/UserDefined/.gitkeep", "")
            for section_name, patterns in sections.items():
                for pat_name, sql_body in patterns:
                    base = f"{ROOT}/sqlPatterns/{db_name}/{section_name}/{pat_name}_0.1"
                    zf.writestr(
                        f"{base}.item",
                        _sql_pattern_item(pat_name, db_name, sql_body),
                    )
                    zf.writestr(base, _sql_pattern_props(pat_name))

    buf.seek(0)
    return buf.read()


# ─────────────────────────────────────────────────────────────────────────────
# CLAUDE AI PARSER
# ─────────────────────────────────────────────────────────────────────────────

def _parse_talend_blocks(raw: str) -> dict:
    """
    Parse Claude output into structured blocks.

    Delimiter format:
      ##PROCESS:JobName##              → process/ root
      ##PROCESS:FolderName/JobName##   → process/FolderName/
      ##CONTEXT:CtxName##             → context/ flat
      ##ROUTINE:RoutName##            → code/routines/ flat
      ##CONNECTION:ConnName##         → metadata/connections/ flat
      ##FILE_DELIMITED:Name##         → metadata/fileDelimited/ flat
      ##FILE_EXCEL:Name##             → metadata/fileExcel/ flat
    """
    result = {k: [] for k in ["PROCESS", "CONTEXT", "ROUTINE", "CONNECTION", "FILE_DELIMITED", "FILE_EXCEL"]}

    pattern = re.compile(
        r"##(PROCESS|CONTEXT|ROUTINE|CONNECTION|FILE_DELIMITED|FILE_EXCEL):([^#\n]+)##\s*(.*?)\s*##END##",
        re.DOTALL | re.IGNORECASE,
    )

    for m in pattern.finditer(raw):
        btype   = m.group(1).upper()
        raw_name = m.group(2).strip()
        content  = m.group(3).strip()

        if not content.startswith("<?xml"):
            content = '<?xml version="1.0" encoding="UTF-8"?>\n' + content

        # Support folder/name in PROCESS blocks
        folder = ""
        name   = raw_name
        if btype == "PROCESS" and "/" in raw_name:
            parts  = raw_name.split("/", 1)
            folder = parts[0].strip()
            name   = parts[1].strip()

        if btype in result:
            result[btype].append({"name": name, "folder": folder, "content": content, "version": "0.1"})

    return result


# ─────────────────────────────────────────────────────────────────────────────
# SYSTEM PROMPT
# ─────────────────────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = '''\
You are an expert Talend Open Studio 8 developer for the project GAININSIGHTS (Talend Cloud 8.0.1).

Generate a COMPLETE Talend project using EXACT delimiter blocks.

═══ DELIMITER FORMAT ═══════════════════════════════════════════════════════════

For a process job at root level:
##PROCESS:JobName##
<?xml version="1.0" encoding="UTF-8"?>
<xmi:XMI ...> full .item XML </xmi:XMI>
##END##

For a process job inside a subfolder (e.g. PROD_JOBS):
##PROCESS:PROD_JOBS/JobName##
<?xml version="1.0" encoding="UTF-8"?>
<xmi:XMI ...> full .item XML </xmi:XMI>
##END##

##CONTEXT:ContextName##
<?xml version="1.0" encoding="UTF-8"?>
<xmi:XMI ...> full .item XML </xmi:XMI>
##END##

##CONNECTION:ConnectionName##
<?xml version="1.0" encoding="UTF-8"?>
<xmi:XMI ...> full .item XML </xmi:XMI>
##END##

##ROUTINE:RoutineName##
<?xml version="1.0" encoding="UTF-8"?>
<xmi:XMI ...> full .item XML </xmi:XMI>
##END##

##FILE_DELIMITED:FileName##
<?xml version="1.0" encoding="UTF-8"?>
<xmi:XMI ...> full .item XML </xmi:XMI>
##END##

##FILE_EXCEL:ExcelName##
<?xml version="1.0" encoding="UTF-8"?>
<xmi:XMI ...> full .item XML </xmi:XMI>
##END##

═══ STRICT RULES ═══════════════════════════════════════════════════════════════
1. NO markdown, NO explanation — ONLY the delimiter blocks.
2. Every block: starts with ##TYPE:Name## ends with ##END## on its own line.
3. Every XML must start with <?xml version="1.0" encoding="UTF-8"?>
4. Generate ORDER: CONNECTION → CONTEXT → ROUTINE → PROCESS
5. Always generate at least: 1 CONNECTION, 1 CONTEXT (Default), 1 PROCESS.

═══ PROCESS .item TEMPLATE ═════════════════════════════════════════════════════

<?xml version="1.0" encoding="UTF-8"?>
<xmi:XMI xmi:version="2.0" xmlns:xmi="http://www.omg.org/XMI"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xmlns:TalendProperties="http://www.talend.org/properties"
    xmlns:TalendFileProperties="http://www.talend.org/fileproperties">
  <TalendProperties:Property xmi:id="_PROPID001"
      label="JOB_NAME" version="0.1" statusCode="PROD"
      displayName="JOB_NAME" purpose="ETL"
      description="Generated by GainInsights AI"
      creationDate="20250407 17:45:00"
      modificationDate="20250407 17:45:00"
      author="_Rt5XciZtEeyMZIJ95Km03Q">
    <item xsi:type="TalendFileProperties:ProcessItem" xmi:id="_ITEMID001">
      <process xsi:type="TalendMDM:ProcessType"
          xmlns:TalendMDM="http://www.talend.org/mdm"
          label="JOB_NAME" version="0.1"
          defaultContext="Default" jobType="STANDARD">
        <parameters>
          <elementParameter field="TEXT" name="LABEL" value="JOB_NAME"/>
          <elementParameter field="CHECK" name="MULTI_THREAD_EXECATION" value="false"/>
          <elementParameter field="CHECK" name="IMPLICIT_TCONTEXTLOAD" value="false"/>
          <elementParameter field="TEXT" name="EXEC_ENGINE" value="LOCAL"/>
        </parameters>
        <node componentName="tDBInput" componentVersion="0.29"
            offsetLabelX="0" offsetLabelY="0" posX="128" posY="200">
          <elementParameter field="TEXT" name="LABEL" value=""/>
          <elementParameter field="TEXT" name="DB_TYPE" value="MySQL"/>
          <elementParameter field="TEXT" name="QUERY" value="&quot;SELECT * FROM source_table&quot;"/>
          <metadata connector="FLOW" label="row1" name="row1">
            <column comment="" key="true" length="10" name="id"
                nullable="false" precision="0" type="id_Integer"/>
            <column comment="" key="false" length="100" name="name"
                nullable="true" precision="0" type="id_String"/>
          </metadata>
        </node>
        <node componentName="tMap" componentVersion="0.54"
            offsetLabelX="0" offsetLabelY="0" posX="368" posY="200">
          <elementParameter field="TEXT" name="LABEL" value=""/>
        </node>
        <node componentName="tDBOutput" componentVersion="0.29"
            offsetLabelX="0" offsetLabelY="0" posX="608" posY="200">
          <elementParameter field="TEXT" name="LABEL" value=""/>
          <elementParameter field="TEXT" name="TABLE" value="&quot;target_table&quot;"/>
          <elementParameter field="TEXT" name="ACTION_ON_TABLE" value="DEFAULT"/>
        </node>
        <connection connectorName="FLOW" label="row1" lineStyle="0"
            source="tDBInput_1" target="tMap_1"/>
        <connection connectorName="FLOW" label="out1" lineStyle="0"
            source="tMap_1" target="tDBOutput_1"/>
      </process>
    </item>
  </TalendProperties:Property>
</xmi:XMI>

Component posX: 128, 368, 608, 848, 1088 (increment 240 each step)
Component posY: 200 for main flow, 400 for error/parallel flow

Correct Talend 8 component names:
  Input:     tDBInput tFileInputDelimited tFileInputExcel tFixedFlowInput
  Transform: tMap tFilterRow tSortRow tAggregateRow tUniqRow tConvertType tReplaceList
  Output:    tDBOutput tFileOutputDelimited tFileOutputExcel tLogRow
  Control:   tRunJob tFlowToIterate tContextLoad tSetGlobalVar

═══ CONTEXT .item TEMPLATE ═════════════════════════════════════════════════════

<?xml version="1.0" encoding="UTF-8"?>
<xmi:XMI xmi:version="2.0" xmlns:xmi="http://www.omg.org/XMI"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xmlns:TalendProperties="http://www.talend.org/properties">
  <TalendProperties:Property xmi:id="_CTXPROP001"
      label="Default" version="0.1" statusCode="PROD"
      displayName="Default" purpose="Context Group"
      author="_Rt5XciZtEeyMZIJ95Km03Q">
    <item xsi:type="TalendProperties:ContextItem" xmi:id="_CTXITEM001">
      <context name="Default" confirmationNeeded="false">
        <contextParameter comment="Source DB Host" name="SRC_HOST"
            prompt="" promptNeeded="false" type="id_String" value="localhost"/>
        <contextParameter comment="Source DB Port" name="SRC_PORT"
            prompt="" promptNeeded="false" type="id_Integer" value="3306"/>
        <contextParameter comment="Source DB Name" name="SRC_DB"
            prompt="" promptNeeded="false" type="id_String" value="source_db"/>
        <contextParameter comment="Source DB User" name="SRC_USER"
            prompt="" promptNeeded="false" type="id_String" value="root"/>
        <contextParameter comment="Source DB Password" name="SRC_PASS"
            prompt="" promptNeeded="false" type="id_Password" value="password"/>
        <contextParameter comment="Target DB Host" name="TGT_HOST"
            prompt="" promptNeeded="false" type="id_String" value="localhost"/>
        <contextParameter comment="Target DB Port" name="TGT_PORT"
            prompt="" promptNeeded="false" type="id_Integer" value="5432"/>
        <contextParameter comment="Target DB Name" name="TGT_DB"
            prompt="" promptNeeded="false" type="id_String" value="target_db"/>
        <contextParameter comment="Target DB User" name="TGT_USER"
            prompt="" promptNeeded="false" type="id_String" value="etl_user"/>
        <contextParameter comment="Target DB Password" name="TGT_PASS"
            prompt="" promptNeeded="false" type="id_Password" value="password"/>
        <contextParameter comment="Input path" name="INPUT_PATH"
            prompt="" promptNeeded="false" type="id_String" value="C:/data/input/"/>
        <contextParameter comment="Output path" name="OUTPUT_PATH"
            prompt="" promptNeeded="false" type="id_String" value="C:/data/output/"/>
      </context>
    </item>
  </TalendProperties:Property>
</xmi:XMI>

Adapt parameter names and values to match the job requirements.
If Prod/Dev mentioned, add multiple <context> blocks inside the same ContextItem.

═══ CONNECTION .item TEMPLATE ══════════════════════════════════════════════════

<?xml version="1.0" encoding="UTF-8"?>
<xmi:XMI xmi:version="2.0" xmlns:xmi="http://www.omg.org/XMI"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xmlns:TalendProperties="http://www.talend.org/properties"
    xmlns:Metadata="http://www.talend.org/metadata">
  <TalendProperties:Property xmi:id="_CONNPROP001"
      label="ConnectionName" version="0.1" statusCode="PROD"
      displayName="ConnectionName"
      author="_Rt5XciZtEeyMZIJ95Km03Q">
    <item xsi:type="Metadata:DatabaseConnection" xmi:id="_CONNITEM001"
        dbType="MySQL"
        URL="jdbc:mysql://context.SRC_HOST:context.SRC_PORT/context.SRC_DB"
        username="context.SRC_USER"
        password="context.SRC_PASS"
        schema="public"
        driverClass="com.mysql.cj.jdbc.Driver"
        name="ConnectionName">
      <tables xmi:id="_TBL001" label="sample_table" name="sample_table">
        <columns xmi:id="_COL001" label="id" name="id"
            key="true" nullable="false" length="10" type="id_Integer"/>
        <columns xmi:id="_COL002" label="name" name="name"
            key="false" nullable="true" length="100" type="id_String"/>
      </tables>
    </item>
  </TalendProperties:Property>
</xmi:XMI>

dbType values: MySQL, PostgreSQL, Oracle, MSSQL, DB2, Sybase, SQLite, Snowflake
'''


# ─────────────────────────────────────────────────────────────────────────────
# CLAUDE API CALL
# ─────────────────────────────────────────────────────────────────────────────

def _call_claude(instructions: str) -> tuple[str, int, int]:
    settings_obj = UserApiSettings.objects.last()
    if not settings_obj:
        raise RuntimeError("UserApiSettings not configured.")

    user_prompt = (
        "Read the following ETL instructions carefully.\n"
        "Generate a COMPLETE Talend project with ALL required files.\n"
        "Generate ORDER: CONNECTION blocks first → CONTEXT → ROUTINE (if needed) → PROCESS jobs.\n"
        "For PROCESS jobs that belong in a group, use ##PROCESS:FolderName/JobName## format.\n\n"
        f"--- INSTRUCTIONS ---\n{instructions}\n---\n\n"
        "Every ##TYPE:Name## block MUST end with ##END## on its own line.\n"
        "ALL XML must be complete and valid."
    )

    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key":         settings_obj.api_key,
            "anthropic-version": "2023-06-01",
            "Content-Type":      "application/json",
        },
        json={
            "model":      settings_obj.model,
            "max_tokens": settings_obj.max_tokens,
            "system":     _SYSTEM_PROMPT,
            "messages":   [{"role": "user", "content": user_prompt}],
        },
        timeout=settings_obj.timeout,
        verify=False,
    )

    if resp.status_code != 200:
        raise RuntimeError(f"Claude API error {resp.status_code}: {resp.text}")

    body  = resp.json()
    usage = body.get("usage", {})
    return body["content"][0]["text"].strip(), usage.get("input_tokens", 0), usage.get("output_tokens", 0)


def _save_to_db(parsed: dict, zip_bytes: bytes, in_tok: int, out_tok: int, dur_ms: int):
    for btype, items in parsed.items():
        for item in items:
            log = GenerationLog.objects.create(
                filename      = f"{item['name']}_0.1.item",
                file_type     = btype[:4].lower(),
                status        = "ok",
                step_count    = item["content"].count("<node "),
                input_tokens  = in_tok,
                output_tokens = out_tok,
                duration_ms   = dur_ms,
            )
            gf = GeneratedFile(
                name       = item["name"],
                file_type  = btype[:4].lower(),
                size_bytes = len(item["content"].encode()),
                source     = "talend_project_generator",
                log        = log,
            )
            gf.file.save(f"{item['name']}_0.1.item", ContentFile(item["content"].encode()), save=True)


# ─────────────────────────────────────────────────────────────────────────────
# DJANGO VIEWS
# ─────────────────────────────────────────────────────────────────────────────

@login_required(login_url='/')
def talend_generator_page(request):
    settings_obj = UserApiSettings.objects.last()
    model_name   = (settings_obj.model if settings_obj else "—").replace("claude-", "")
    return render(request, "talend_generator.html", {
        "active_page": "talend_generator",
        "model":       model_name,
    })


@login_required(login_url='/')
@csrf_exempt
@require_http_methods(["POST"])
def generate_talend_files(request):
    # 1. Read instructions
    instructions  = ""
    uploaded_file = request.FILES.get("steps_file")
    if uploaded_file:
        try:
            instructions = uploaded_file.read().decode("utf-8")
        except UnicodeDecodeError:
            return JsonResponse({"error": "File must be UTF-8 encoded."}, status=400)
    else:
        instructions = request.POST.get("steps_text", "").strip()

    if not instructions:
        return JsonResponse({"error": "No instructions provided."}, status=400)

    # 2. Call Claude
    t0 = int(time.time() * 1000)
    try:
        raw, in_tok, out_tok = _call_claude(instructions)
    except RuntimeError as exc:
        GenerationLog.objects.create(
            filename="talend_project.zip", file_type="zip",
            status="error", error_message=str(exc)[:500],
            duration_ms=int(time.time() * 1000) - t0,
        )
        return JsonResponse({"error": str(exc)}, status=500)

    dur_ms = int(time.time() * 1000) - t0

    # 3. Parse
    parsed = _parse_talend_blocks(raw)
    total  = sum(len(v) for v in parsed.values())

    if total == 0:
        GenerationLog.objects.create(
            filename="talend_project.zip", file_type="zip",
            status="error", error_message="No delimiter blocks found in Claude output",
            input_tokens=in_tok, output_tokens=out_tok, duration_ms=dur_ms,
        )
        return JsonResponse({"error": "No Talend blocks parsed.", "debug": raw[:800]}, status=500)

    # 4. Build ZIP
    zip_bytes = _build_talend_project_zip(parsed)

    # 5. Save
    _save_to_db(parsed, zip_bytes, in_tok, out_tok, dur_ms)

    # 6. Return
    prefs, _ = UserPreference.objects.get_or_create(id=1)
    zip_name = (prefs.folder_name or "GAININSIGHTS") + "_talend_project.zip"

    resp = HttpResponse(zip_bytes, content_type="application/zip")
    resp["Content-Disposition"]           = f'attachment; filename="{zip_name}"'
    resp["X-Process-Count"]               = str(len(parsed["PROCESS"]))
    resp["X-Context-Count"]               = str(len(parsed["CONTEXT"]))
    resp["X-Routine-Count"]               = str(len(parsed["ROUTINE"]))
    resp["X-Connection-Count"]            = str(len(parsed["CONNECTION"]))
    resp["Access-Control-Expose-Headers"] = "X-Process-Count,X-Context-Count,X-Routine-Count,X-Connection-Count"
    return resp


@login_required(login_url='/')
def upload_talend(request):
    if request.method == "POST":
        uploaded_file = request.FILES.get("steps_file")
        if not uploaded_file:
            return JsonResponse({"error": "No file uploaded"}, status=400)
        try:
            instructions = uploaded_file.read().decode("utf-8")
        except UnicodeDecodeError:
            return JsonResponse({"error": "File must be UTF-8 encoded."}, status=400)

        t0 = int(time.time() * 1000)
        try:
            raw, in_tok, out_tok = _call_claude(instructions)
        except RuntimeError as exc:
            return JsonResponse({"error": str(exc)}, status=500)

        parsed    = _parse_talend_blocks(raw)
        zip_bytes = _build_talend_project_zip(parsed)
        _save_to_db(parsed, zip_bytes, in_tok, out_tok, int(time.time() * 1000) - t0)

        resp = HttpResponse(zip_bytes, content_type="application/zip")
        resp["Content-Disposition"] = 'attachment; filename="GAININSIGHTS_talend_project.zip"'
        return resp

    settings_obj = UserApiSettings.objects.last()
    model_name   = (settings_obj.model if settings_obj else "—").replace("claude-", "")
    return render(request, "talend_generator.html", {"active_page": "talend_generator", "model": model_name})


@login_required(login_url='/')
def dashboard_talend_stats(request):
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