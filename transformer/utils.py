import re
import requests
import psycopg2
from django.conf import settings
from .models import UserApiSettings, GeneratedFile
import time
from django.core.files.base import ContentFile
from django.utils.text import slugify 

#===========================KTR==========================================================================

REFERENCE_KTR = """<?xml version="1.0" encoding="UTF-8"?>
                <transformation>
                <info>
                    <name>trans_cleanse_employee</name>
                    <trans_type>Normal</trans_type>
                    <trans_status>0</trans_status>
                    <directory>/</directory>
                    <size_rowset>10000</size_rowset>
                    <sleep_time_empty>50</sleep_time_empty>
                    <sleep_time_full>50</sleep_time_full>
                    <unique_connections>N</unique_connections>
                    <feedback_shown>Y</feedback_shown>
                    <feedback_size>50000</feedback_size>
                    <using_thread_priorities>Y</using_thread_priorities>
                    <capture_step_performance>N</capture_step_performance>
                </info>
                <notepads/>
                <order>
                    <hop><from>CSV file input</from><to>String operations</to><enabled>Y</enabled></hop>
                    <hop><from>String operations</from><to>Modified JavaScript value</to><enabled>Y</enabled></hop>
                    <hop><from>Modified JavaScript value</from><to>Select values</to><enabled>Y</enabled></hop>
                    <hop><from>Select values</from><to>Microsoft Excel writer</to><enabled>Y</enabled></hop>
                </order>
                <step>
                    <name>CSV file input</name>
                    <type>CsvInput</type>
                    <distribute>N</distribute>
                    <copies>1</copies>
                    <partitioning><method>none</method><schema_name/></partitioning>
                    <filename>C:\\input\\data.csv</filename>
                    <separator>,</separator>
                    <enclosure>"</enclosure>
                    <header>Y</header>
                    <buffer_size>50000</buffer_size>
                    <lazy_conversion>Y</lazy_conversion>
                    <parallel>N</parallel>
                    <format>mixed</format>
                    <fields>
                    <field><name>emp_id</name><type>Integer</type><format>#</format><length>15</length><precision>0</precision><trim_type>none</trim_type></field>
                    <field><name>emp_name</name><type>String</type><length>100</length><precision>-1</precision><trim_type>none</trim_type></field>
                    <field><name>department</name><type>String</type><length>50</length><precision>-1</precision><trim_type>none</trim_type></field>
                    <field><name>salary</name><type>Integer</type><format>#</format><length>15</length><precision>0</precision><trim_type>none</trim_type></field>
                    </fields>
                    <GUI><xloc>208</xloc><yloc>80</yloc><draw>Y</draw></GUI>
                </step>
                <step>
                    <name>String operations</name>
                    <type>StringOperations</type>
                    <distribute>N</distribute>
                    <copies>1</copies>
                    <partitioning><method>none</method><schema_name/></partitioning>
                    <fields>
                    <field>
                        <in_stream_name>emp_name</in_stream_name>
                        <out_stream_name/>
                        <trim_type>both</trim_type>
                        <lower_upper/>
                        <padding_type/>
                        <pad_char/>
                        <pad_len/>
                        <init_cap/>
                        <mask_xml/>
                        <digits/>
                        <remove_special_characters/>
                    </field>
                    </fields>
                    <GUI><xloc>384</xloc><yloc>80</yloc><draw>Y</draw></GUI>
                </step>
                <step>
                    <name>Modified JavaScript value</name>
                    <type>ScriptValueMod</type>
                    <distribute>Y</distribute>
                    <copies>1</copies>
                    <partitioning><method>none</method><schema_name/></partitioning>
                    <compatible>N</compatible>
                    <optimizationLevel>9</optimizationLevel>
                    <jsScripts>
                    <jsScript>
                        <jsScript_type>0</jsScript_type>
                        <jsScript_name>Script 1</jsScript_name>
                        <jsScript_script>
                var department_1;
                if (department == null || department == "") {
                department_1 = "UNKNOWN";
                } else {
                department_1 = department;
                }
                        </jsScript_script>
                    </jsScript>
                    </jsScripts>
                    <fields>
                    <field><name>department_1</name><rename>department_1</rename><type>String</type><length>-1</length><precision>-1</precision><replace>N</replace></field>
                    </fields>
                    <GUI><xloc>592</xloc><yloc>80</yloc><draw>Y</draw></GUI>
                </step>
                <step>
                    <name>Select values</name>
                    <type>SelectValues</type>
                    <distribute>N</distribute>
                    <copies>1</copies>
                    <partitioning><method>none</method><schema_name/></partitioning>
                    <fields>
                    <field><name>emp_id</name></field>
                    <field><name>emp_name</name></field>
                    <field><name>department_1</name><rename>department</rename></field>
                    <field><name>salary</name></field>
                    <select_unspecified>N</select_unspecified>
                    </fields>
                    <GUI><xloc>592</xloc><yloc>192</yloc><draw>Y</draw></GUI>
                </step>
                <step>
                    <name>Microsoft Excel writer</name>
                    <type>TypeExitExcelWriterStep</type>
                    <distribute>Y</distribute>
                    <copies>1</copies>
                    <partitioning><method>none</method><schema_name/></partitioning>
                    <header>Y</header>
                    <footer>N</footer>
                    <makeSheetActive>Y</makeSheetActive>
                    <startingCell>A1</startingCell>
                    <rowWritingMethod>overwrite</rowWritingMethod>
                    <appendLines>N</appendLines>
                    <add_to_result_filenames>Y</add_to_result_filenames>
                    <file>
                    <name>C:\\output\\employee_final</name>
                    <extention>xls</extention>
                    <create_parent>N</create_parent>
                    <sheetname>Sheet1</sheetname>
                    <if_file_exists>new</if_file_exists>
                    <if_sheet_exists>new</if_sheet_exists>
                    </file>
                    <fields/>
                    <GUI><xloc>384</xloc><yloc>192</yloc><draw>Y</draw></GUI>
                </step>
                <step_error_handling>
                    <error>
                    <source_step>CSV file input</source_step>
                    <target_step>Write to log</target_step>
                    <is_enabled>Y</is_enabled>
                    </error>
                </step_error_handling>
                <slave_transformation>N</slave_transformation>
                </transformation>"""


SYSTEM_PROMPT_KTR = """You are an expert in Pentaho Data Integration (PDI/Kettle).
            Your job is to generate valid Pentaho .ktr transformation XML files that can be opened directly in Spoon.

            STRICT RULES:
            1. Output ONLY valid XML. No explanation, no markdown, no code fences.
            2. Follow the exact XML structure and tag names from the reference template provided.
            3. Every <step> must have: <name>, <type>, <distribute>, <copies>, <partitioning>, <GUI> tags.
            4. Every <hop> must have: <from>, <to>, <enabled> tags.
            5. GUI <xloc> and <yloc> must be spaced logically (increment by ~180 per step).
            6. Use correct Pentaho step type names:
            - CSV Input       → CsvInput
            - String Ops      → StringOperations
            - JavaScript      → ScriptValueMod
            - Select Values   → SelectValues
            - Excel Writer    → TypeExitExcelWriterStep
            - Text File Input → TextFileInput
            - Table Output    → TableOutput
            """

#========================== sort option prompt =========================================================

            # 7. IMPORTANT GROUP BY RULE:
            # - Whenever a Group By step is used, ALWAYS add a "Sort Rows" step before it.
            # - The Sort Rows step must sort the data based on the same fields used in the Group By keys.
            # - The hop order must be: previous step → Sort Rows → Group By.
            # - Without sorting, Group By must NOT be used.

            # 8. Ensure Sort Rows step type is correctly defined and includes sorting fields matching Group By columns.

#=======================================================================================================


#===========================KJB==========================================================================


REFERENCE_KJB = r"""<?xml version="1.0" encoding="UTF-8"?>
                    <job>
                    <name>JOB_CUSTOMER_EXCEL_GENERATION</name>
                    <description/>
                    <extended_description/>
                    <job_version/>
                    <directory>/</directory>
                    <created_user>-</created_user>
                    <created_date>2026/02/12 14:04:51.044</created_date>
                    <modified_user>-</modified_user>
                    <modified_date>2026/02/12 14:04:51.044</modified_date>
                    <parameters>
                        </parameters>
                    <slaveservers>
                        </slaveservers>
                    <job-log-table>
                        <connection/>
                        <schema/>
                        <table/>
                        <size_limit_lines/>
                        <interval/>
                        <timeout_days/>
                        <field>
                        <id>ID_JOB</id>
                        <enabled>Y</enabled>
                        <name>ID_JOB</name>
                        </field>
                        <field>
                        <id>CHANNEL_ID</id>
                        <enabled>Y</enabled>
                        <name>CHANNEL_ID</name>
                        </field>
                        <field>
                        <id>JOBNAME</id>
                        <enabled>Y</enabled>
                        <name>JOBNAME</name>
                        </field>
                        <field>
                        <id>STATUS</id>
                        <enabled>Y</enabled>
                        <name>STATUS</name>
                        </field>
                        <field>
                        <id>LINES_READ</id>
                        <enabled>Y</enabled>
                        <name>LINES_READ</name>
                        </field>
                        <field>
                        <id>LINES_WRITTEN</id>
                        <enabled>Y</enabled>
                        <name>LINES_WRITTEN</name>
                        </field>
                        <field>
                        <id>LINES_UPDATED</id>
                        <enabled>Y</enabled>
                        <name>LINES_UPDATED</name>
                        </field>
                        <field>
                        <id>LINES_INPUT</id>
                        <enabled>Y</enabled>
                        <name>LINES_INPUT</name>
                        </field>
                        <field>
                        <id>LINES_OUTPUT</id>
                        <enabled>Y</enabled>
                        <name>LINES_OUTPUT</name>
                        </field>
                        <field>
                        <id>LINES_REJECTED</id>
                        <enabled>Y</enabled>
                        <name>LINES_REJECTED</name>
                        </field>
                        <field>
                        <id>ERRORS</id>
                        <enabled>Y</enabled>
                        <name>ERRORS</name>
                        </field>
                        <field>
                        <id>STARTDATE</id>
                        <enabled>Y</enabled>
                        <name>STARTDATE</name>
                        </field>
                        <field>
                        <id>ENDDATE</id>
                        <enabled>Y</enabled>
                        <name>ENDDATE</name>
                        </field>
                        <field>
                        <id>LOGDATE</id>
                        <enabled>Y</enabled>
                        <name>LOGDATE</name>
                        </field>
                        <field>
                        <id>DEPDATE</id>
                        <enabled>Y</enabled>
                        <name>DEPDATE</name>
                        </field>
                        <field>
                        <id>REPLAYDATE</id>
                        <enabled>Y</enabled>
                        <name>REPLAYDATE</name>
                        </field>
                        <field>
                        <id>LOG_FIELD</id>
                        <enabled>Y</enabled>
                        <name>LOG_FIELD</name>
                        </field>
                        <field>
                        <id>EXECUTING_SERVER</id>
                        <enabled>N</enabled>
                        <name>EXECUTING_SERVER</name>
                        </field>
                        <field>
                        <id>EXECUTING_USER</id>
                        <enabled>N</enabled>
                        <name>EXECUTING_USER</name>
                        </field>
                        <field>
                        <id>START_JOB_ENTRY</id>
                        <enabled>N</enabled>
                        <name>START_JOB_ENTRY</name>
                        </field>
                        <field>
                        <id>CLIENT</id>
                        <enabled>N</enabled>
                        <name>CLIENT</name>
                        </field>
                    </job-log-table>
                    <jobentry-log-table>
                        <connection/>
                        <schema/>
                        <table/>
                        <timeout_days/>
                        <field>
                        <id>ID_BATCH</id>
                        <enabled>Y</enabled>
                        <name>ID_BATCH</name>
                        </field>
                        <field>
                        <id>CHANNEL_ID</id>
                        <enabled>Y</enabled>
                        <name>CHANNEL_ID</name>
                        </field>
                        <field>
                        <id>LOG_DATE</id>
                        <enabled>Y</enabled>
                        <name>LOG_DATE</name>
                        </field>
                        <field>
                        <id>JOBNAME</id>
                        <enabled>Y</enabled>
                        <name>TRANSNAME</name>
                        </field>
                        <field>
                        <id>JOBENTRYNAME</id>
                        <enabled>Y</enabled>
                        <name>STEPNAME</name>
                        </field>
                        <field>
                        <id>LINES_READ</id>
                        <enabled>Y</enabled>
                        <name>LINES_READ</name>
                        </field>
                        <field>
                        <id>LINES_WRITTEN</id>
                        <enabled>Y</enabled>
                        <name>LINES_WRITTEN</name>
                        </field>
                        <field>
                        <id>LINES_UPDATED</id>
                        <enabled>Y</enabled>
                        <name>LINES_UPDATED</name>
                        </field>
                        <field>
                        <id>LINES_INPUT</id>
                        <enabled>Y</enabled>
                        <name>LINES_INPUT</name>
                        </field>
                        <field>
                        <id>LINES_OUTPUT</id>
                        <enabled>Y</enabled>
                        <name>LINES_OUTPUT</name>
                        </field>
                        <field>
                        <id>LINES_REJECTED</id>
                        <enabled>Y</enabled>
                        <name>LINES_REJECTED</name>
                        </field>
                        <field>
                        <id>ERRORS</id>
                        <enabled>Y</enabled>
                        <name>ERRORS</name>
                        </field>
                        <field>
                        <id>RESULT</id>
                        <enabled>Y</enabled>
                        <name>RESULT</name>
                        </field>
                        <field>
                        <id>NR_RESULT_ROWS</id>
                        <enabled>Y</enabled>
                        <name>NR_RESULT_ROWS</name>
                        </field>
                        <field>
                        <id>NR_RESULT_FILES</id>
                        <enabled>Y</enabled>
                        <name>NR_RESULT_FILES</name>
                        </field>
                        <field>
                        <id>LOG_FIELD</id>
                        <enabled>N</enabled>
                        <name>LOG_FIELD</name>
                        </field>
                        <field>
                        <id>COPY_NR</id>
                        <enabled>N</enabled>
                        <name>COPY_NR</name>
                        </field>
                    </jobentry-log-table>
                    <channel-log-table>
                        <connection/>
                        <schema/>
                        <table/>
                        <timeout_days/>
                        <field>
                        <id>ID_BATCH</id>
                        <enabled>Y</enabled>
                        <name>ID_BATCH</name>
                        </field>
                        <field>
                        <id>CHANNEL_ID</id>
                        <enabled>Y</enabled>
                        <name>CHANNEL_ID</name>
                        </field>
                        <field>
                        <id>LOG_DATE</id>
                        <enabled>Y</enabled>
                        <name>LOG_DATE</name>
                        </field>
                        <field>
                        <id>LOGGING_OBJECT_TYPE</id>
                        <enabled>Y</enabled>
                        <name>LOGGING_OBJECT_TYPE</name>
                        </field>
                        <field>
                        <id>OBJECT_NAME</id>
                        <enabled>Y</enabled>
                        <name>OBJECT_NAME</name>
                        </field>
                        <field>
                        <id>OBJECT_COPY</id>
                        <enabled>Y</enabled>
                        <name>OBJECT_COPY</name>
                        </field>
                        <field>
                        <id>REPOSITORY_DIRECTORY</id>
                        <enabled>Y</enabled>
                        <name>REPOSITORY_DIRECTORY</name>
                        </field>
                        <field>
                        <id>FILENAME</id>
                        <enabled>Y</enabled>
                        <name>FILENAME</name>
                        </field>
                        <field>
                        <id>OBJECT_ID</id>
                        <enabled>Y</enabled>
                        <name>OBJECT_ID</name>
                        </field>
                        <field>
                        <id>OBJECT_REVISION</id>
                        <enabled>Y</enabled>
                        <name>OBJECT_REVISION</name>
                        </field>
                        <field>
                        <id>PARENT_CHANNEL_ID</id>
                        <enabled>Y</enabled>
                        <name>PARENT_CHANNEL_ID</name>
                        </field>
                        <field>
                        <id>ROOT_CHANNEL_ID</id>
                        <enabled>Y</enabled>
                        <name>ROOT_CHANNEL_ID</name>
                        </field>
                    </channel-log-table>
                    <pass_batchid>N</pass_batchid>
                    <entries>
                        <entry>
                        <name>RUN EXCEL SCRIPT</name>
                        <description/>
                        <type>SHELL</type>
                        <attributes/>
                        <filename>C:\Users\prabhakaranp\Documents\prabhakaran\Q22\create_excel_from_csv.py</filename>
                        <work_directory/>
                        <arg_from_previous>N</arg_from_previous>
                        <exec_per_row>N</exec_per_row>
                        <set_logfile>N</set_logfile>
                        <logfile/>
                        <set_append_logfile>N</set_append_logfile>
                        <logext/>
                        <add_date>N</add_date>
                        <add_time>N</add_time>
                        <insertScript>N</insertScript>
                        <script/>
                        <loglevel>Basic</loglevel>
                        <parallel>N</parallel>
                        <draw>Y</draw>
                        <nr>0</nr>
                        <xloc>560</xloc>
                        <yloc>96</yloc>
                        <attributes_kjc/>
                        </entry>
                        <entry>
                        <name>Start</name>
                        <description/>
                        <type>SPECIAL</type>
                        <attributes/>
                        <start>Y</start>
                        <dummy>N</dummy>
                        <repeat>N</repeat>
                        <schedulerType>0</schedulerType>
                        <intervalSeconds>0</intervalSeconds>
                        <intervalMinutes>60</intervalMinutes>
                        <hour>12</hour>
                        <minutes>0</minutes>
                        <weekDay>1</weekDay>
                        <DayOfMonth>1</DayOfMonth>
                        <parallel>N</parallel>
                        <draw>Y</draw>
                        <nr>0</nr>
                        <xloc>144</xloc>
                        <yloc>96</yloc>
                        <attributes_kjc/>
                        </entry>
                        <entry>
                        <name>TRF PREPARE CUSTOMER SPLIT DATA</name>
                        <description/>
                        <type>TRANS</type>
                        <attributes/>
                        <specification_method>filename</specification_method>
                        <trans_object_id/>
                        <filename>${Internal.Entry.Current.Directory}/TRF_EXPORT_CUSTOMER_WITH_SHEET_INFO.ktr</filename>
                        <transname/>
                        <arg_from_previous>N</arg_from_previous>
                        <params_from_previous>N</params_from_previous>
                        <exec_per_row>N</exec_per_row>
                        <clear_rows>N</clear_rows>
                        <clear_files>N</clear_files>
                        <set_logfile>N</set_logfile>
                        <logfile/>
                        <logext/>
                        <add_date>N</add_date>
                        <add_time>N</add_time>
                        <loglevel>Basic</loglevel>
                        <cluster>N</cluster>
                        <slave_server_name/>
                        <set_append_logfile>N</set_append_logfile>
                        <wait_until_finished>Y</wait_until_finished>
                        <follow_abort_remote>N</follow_abort_remote>
                        <create_parent_folder>N</create_parent_folder>
                        <logging_remote_work>N</logging_remote_work>
                        <run_configuration>Pentaho local</run_configuration>
                        <suppress_result_data>N</suppress_result_data>
                        <parameters>
                            <pass_all_parameters>Y</pass_all_parameters>
                        </parameters>
                        <parallel>N</parallel>
                        <draw>Y</draw>
                        <nr>0</nr>
                        <xloc>336</xloc>
                        <yloc>96</yloc>
                        <attributes_kjc/>
                        </entry>
                        <entry>
                        <name>Success</name>
                        <description/>
                        <type>SUCCESS</type>
                        <attributes/>
                        <parallel>N</parallel>
                        <draw>Y</draw>
                        <nr>0</nr>
                        <xloc>720</xloc>
                        <yloc>96</yloc>
                        <attributes_kjc/>
                        </entry>
                        <entry>
                        <name>LOG EXCEL GENERATION FAILURE</name>
                        <description/>
                        <type>WRITE_TO_LOG</type>
                        <attributes/>
                        <logmessage>Job failed during customer Excel generation process.
                    Please check transformation (TRF_SPLIT_CUSTOMER_DATA) or Python execution step (RUN_PYTHON_EXCEL_GENERATION).
                    Review log details for error information.
                    </logmessage>
                        <loglevel>Error</loglevel>
                        <logsubject>CUSTOMER EXCEL JOB ERROR</logsubject>
                        <parallel>N</parallel>
                        <draw>Y</draw>
                        <nr>0</nr>
                        <xloc>336</xloc>
                        <yloc>240</yloc>
                        <attributes_kjc/>
                        </entry>
                        <entry>
                        <name>ABORT CUSTOMER EXCEL JOB</name>
                        <description/>
                        <type>ABORT</type>
                        <attributes/>
                        <message>Customer Excel generation job aborted due to failure in processing.</message>
                        <parallel>N</parallel>
                        <draw>Y</draw>
                        <nr>0</nr>
                        <xloc>560</xloc>
                        <yloc>240</yloc>
                        <attributes_kjc/>
                        </entry>
                    </entries>
                    <hops>
                        <hop>
                        <from>Start</from>
                        <to>TRF PREPARE CUSTOMER SPLIT DATA</to>
                        <from_nr>0</from_nr>
                        <to_nr>0</to_nr>
                        <enabled>Y</enabled>
                        <evaluation>Y</evaluation>
                        <unconditional>Y</unconditional>
                        </hop>
                        <hop>
                        <from>TRF PREPARE CUSTOMER SPLIT DATA</from>
                        <to>RUN EXCEL SCRIPT</to>
                        <from_nr>0</from_nr>
                        <to_nr>0</to_nr>
                        <enabled>Y</enabled>
                        <evaluation>Y</evaluation>
                        <unconditional>N</unconditional>
                        </hop>
                        <hop>
                        <from>RUN EXCEL SCRIPT</from>
                        <to>Success</to>
                        <from_nr>0</from_nr>
                        <to_nr>0</to_nr>
                        <enabled>Y</enabled>
                        <evaluation>Y</evaluation>
                        <unconditional>N</unconditional>
                        </hop>
                        <hop>
                        <from>TRF PREPARE CUSTOMER SPLIT DATA</from>
                        <to>LOG EXCEL GENERATION FAILURE</to>
                        <from_nr>0</from_nr>
                        <to_nr>0</to_nr>
                        <enabled>Y</enabled>
                        <evaluation>N</evaluation>
                        <unconditional>N</unconditional>
                        </hop>
                        <hop>
                        <from>LOG EXCEL GENERATION FAILURE</from>
                        <to>ABORT CUSTOMER EXCEL JOB</to>
                        <from_nr>0</from_nr>
                        <to_nr>0</to_nr>
                        <enabled>Y</enabled>
                        <evaluation>N</evaluation>
                        <unconditional>Y</unconditional>
                        </hop>
                        <hop>
                        <from>RUN EXCEL SCRIPT</from>
                        <to>LOG EXCEL GENERATION FAILURE</to>
                        <from_nr>0</from_nr>
                        <to_nr>0</to_nr>
                        <enabled>Y</enabled>
                        <evaluation>N</evaluation>
                        <unconditional>N</unconditional>
                        </hop>
                    </hops>
                    <notepads>
                    </notepads>
                    <attributes/>
                    </job>"""

SYSTEM_PROMPT_KJB = """You are an expert in Pentaho Data Integration (PDI/Kettle).
Your job is to generate valid Pentaho .kjb job XML files that can be opened directly in Spoon.

STRICT RULES:
1. Output ONLY valid XML. No explanation, no markdown, no code fences.
2. Follow the exact XML structure and tag names from the reference template provided.
3. Every <job> must have: <name>, <entries>, <hops> tags.
"""



# ── Claude AI ────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a PostgreSQL expert.
Generate a valid PostgreSQL CREATE TABLE statement.

RULES:
1. Output ONLY raw SQL – no markdown, no code fences, no explanation.
2. Use SERIAL for auto-increment primary key.
3. Use NUMERIC(10,2) for amounts / decimal fields.
4. Use TIMESTAMP for datetime fields.
5. Use VARCHAR(n) for text, INTEGER for whole numbers, BOOLEAN for true/false flags.
6. Add IF NOT EXISTS.
7. Add NOT NULL where appropriate.
8. Add DEFAULT CURRENT_TIMESTAMP for created_at / create_dtm columns.
9. First token of output must be: CREATE
"""


# def call_claude(instructions: str) -> tuple[str, int, int]:
#     user_prompt = (
#         f"Generate a PostgreSQL CREATE TABLE SQL from these instructions:\n\n"
#         f"--- INSTRUCTIONS ---\n{instructions}\n\n"
#         "Output ONLY the raw SQL starting with: CREATE TABLE"
#     )

#     settings_obj = UserApiSettings.objects.last()

#     model = settings_obj.model
#     max_tokens = settings_obj.max_tokens
#     timeout = settings_obj.timeout
#     api_key = settings_obj.api_key

#     response = requests.post(
#         "https://api.anthropic.com/v1/messages",
#         headers={
#             "x-api-key":         api_key,
#             "anthropic-version": "2023-06-01",
#             "Content-Type":      "application/json",
#         },
#         json={
#             "model":      model,
#             "max_tokens": max_tokens, #2000
#             "system":     SYSTEM_PROMPT,
#             "messages":   [{"role": "user", "content": user_prompt}],
#         },
#         timeout=timeout, #30
#         verify=False,
#     )

#     if response.status_code != 200:
#         raise RuntimeError(
#             f"Claude API error {response.status_code}: {response.text}"
#         )

#     body          = response.json()
#     raw_sql       = body["content"][0]["text"].strip()
#     clean_sql     = re.sub(r"```sql|```", "", raw_sql).strip()

#     usage         = body.get("usage", {})                  # ← ADD
#     input_tokens  = usage.get("input_tokens", 0)           # ← ADD
#     output_tokens = usage.get("output_tokens", 0)          # ← ADD

#     return clean_sql, input_tokens, output_tokens          # ← CHANGED



def call_claude(instructions: str, log=None) -> tuple[str, int, int]:
    user_prompt = (
        f"Generate a PostgreSQL CREATE TABLE SQL from these instructions:\n\n"
        f"--- INSTRUCTIONS ---\n{instructions}\n\n"
        "Output ONLY the raw SQL starting with: CREATE TABLE"
    )

    settings_obj = UserApiSettings.objects.last()
    if not settings_obj:
        raise ValueError("UserApiSettings not configured")

    response = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": settings_obj.api_key,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
        },
        json={
            "model": settings_obj.model,
            "max_tokens": settings_obj.max_tokens,
            "system": SYSTEM_PROMPT,
            "messages": [{"role": "user", "content": user_prompt}],
        },
        timeout=settings_obj.timeout,
        verify=False,
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"Claude API error {response.status_code}: {response.text}"
        )

    body = response.json()

    raw_sql = body["content"][0]["text"].strip()
    clean_sql = re.sub(r"```sql|```", "", raw_sql).strip()

    usage = body.get("usage", {})
    input_tokens = usage.get("input_tokens", 0)
    output_tokens = usage.get("output_tokens", 0)

    # ✅ Generate filename
    timestamp = int(time.time())
    base_name = slugify(instructions[:50]) or "generated_sql"
    filename = f"{base_name}_{timestamp}.sql"

    # ✅ Create file content
    file_content = ContentFile(clean_sql.encode("utf-8"))

    # ✅ Save to DB
    generated_file = GeneratedFile.objects.create(
        name=filename.replace(".sql", ""),
        file_type="sql",
        size_bytes=len(clean_sql.encode("utf-8")),
        source="Table Builder",
        log=log
    )

    # ✅ Save file
    generated_file.file.save(filename, file_content, save=True)

    return clean_sql, input_tokens, output_tokens




# ── PostgreSQL ───────────────────────────────────────────────────────────────

def execute_sql(sql: str) -> dict:
    """
    Execute CREATE TABLE SQL on the configured PostgreSQL database.
    Returns {"table_name": str, "columns": [...]}
    """
    conn = psycopg2.connect(**settings.DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()

    try:
        cur.execute(sql)

        # Extract table name from SQL
        match = re.search(
            r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)",
            sql,
            re.IGNORECASE,
        )
        table_name = match.group(1) if match else "unknown"

        # Fetch column metadata
        cur.execute(
            """
            SELECT column_name,
                   data_type,
                   character_maximum_length,
                   is_nullable
            FROM   information_schema.columns
            WHERE  table_name = %s
            ORDER  BY ordinal_position
            """,
            (table_name,),
        )
        columns = [
            {
                "name":     row[0],
                "type":     row[1],
                "length":   row[2],
                "nullable": row[3],
            }
            for row in cur.fetchall()
        ]

        return {"table_name": table_name, "columns": columns}

    finally:
        cur.close()
        conn.close()


_REFERENCE_KTR = """<?xml version="1.0" encoding="UTF-8"?>
<transformation>
  <info><name>TRANSFORMATION_NAME</name><trans_type>Normal</trans_type></info>
  <order>
    <hop><from>STEP_A</from><to>STEP_B</to><enabled>Y</enabled></hop>
  </order>
  <step>
    <name>STEP_A</name><type>CsvInput</type><distribute>N</distribute><copies>1</copies>
    <partitioning><method>none</method><schema_name/></partitioning>
    <filename>input.csv</filename><separator>,</separator><enclosure>"</enclosure>
    <header>Y</header><parallel>N</parallel><format>mixed</format><fields/>
    <GUI><xloc>100</xloc><yloc>100</yloc><draw>Y</draw></GUI>
  </step>
  <step>
    <name>STEP_B</name><type>TableOutput</type><distribute>Y</distribute><copies>1</copies>
    <partitioning><method>none</method><schema_name/></partitioning>
    <connection>db_conn</connection><table>output_table</table><commit>100</commit>
    <GUI><xloc>300</xloc><yloc>100</yloc><draw>Y</draw></GUI>
  </step>
  <slave_transformation>N</slave_transformation>
</transformation>"""


_SYSTEM_PROMPT = (
    "You are a Pentaho PDI (Kettle) expert.\n\n"
    "Read the user instructions and generate multiple Pentaho transformation (.ktr)\n"
    "and job (.kjb) XML files.\n\n"
    "OUTPUT FORMAT — use these exact delimiters, nothing else:\n\n"
    "##KTR:filename_without_extension##\n"
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
    "<transformation>\n"
    "  ...full valid ktr xml...\n"
    "</transformation>\n"
    "##END##\n\n"
    "##KJB:filename_without_extension##\n"
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
    "<job>\n"
    "  ...full valid kjb xml...\n"
    "</job>\n"
    "##END##\n\n"
    "RULES:\n"
    "1. Use ONLY the delimiter format above — no JSON, no markdown, no explanation.\n"
    "2. Each file starts with ##KTR:name## or ##KJB:name## and ends with ##END##\n"
    "3. The XML inside must be complete and valid.\n"
    "4. Every <step> must have: <name>, <type>, <distribute>, <copies>, <partitioning>, <GUI>\n"
    "5. Every <hop>  must have: <from>, <to>, <enabled>\n"
    "6. Every job <entry> must have: <name>, <type>, <GUI>\n"
    "7. GUI xloc increments by 200 per step.\n"
    "8. Correct Pentaho type names:\n"
    "   CSV Input     -> CsvInput        Table Input  -> TableInput\n"
    "   Table Output  -> TableOutput     Sort Rows    -> SortRows\n"
    "   String Ops    -> StringOperations  Group By   -> GroupBy\n"
    "   JavaScript    -> ScriptValueMod  Select Values -> SelectValues\n"
    "   Filter Rows   -> FilterRows      Excel Writer  -> TypeExitExcelWriterStep\n"
    "   Job Start     -> SPECIAL         Run Trans     -> TRANS    SQL -> SQL\n\n"
    "REFERENCE KTR:\n" + _REFERENCE_KTR + "\n\n"
    "REFERENCE KJB:\n" + REFERENCE_KJB
)


SYSTEM_PROMPT_SQL_PRIVIEW = """You are an expert SQL and Pentaho PDI (Kettle) engineer.

Your job is to read a structured steps file and convert it into a complete, executable SQL script.

STEPS FILE FORMAT:
- SOURCE, table_name, connection, alias, col1;col2;col3, WHERE_condition, OR|AND
- JOIN,   alias_a.col = alias_b.col, INNER|LEFT|RIGHT|FULL
- TARGET, table_name, connection, create|existing|upsert, upsert_key
- OUTPUT, source_column, target_column

COLUMN TYPE INFERENCE RULES (apply to CREATE TABLE columns):
Infer the best SQL data type from the column name using these patterns:

  PRIMARY KEY / ID columns:
    - name ends with _id, _ID, or is exactly "id", "ID"       → INT PRIMARY KEY
    - name ends with _uuid, _guid                              → VARCHAR(36)

  NUMERIC columns:
    - name contains: amount, price, cost, salary, fee, total,
                     balance, rate, discount, tax, revenue      → DECIMAL(15,2)
    - name contains: percentage, percent, pct, ratio            → DECIMAL(5,2)
    - name contains: age, year, qty, quantity, count,
                     num, number, score, rank, grade            → INT
    - name contains: weight, height, latitude, longitude,
                     lat, lng, distance, avg, average           → DOUBLE
    - name contains: is_, has_, flag, active, enabled,
                     deleted, verified, status (boolean-like)   → TINYINT(1)

  DATE / TIME columns:
    - name contains: _date, date_, dob, birth, expiry,
                     created_at, updated_at, deleted_at,
                     joined, start_date, end_date               → DATE
    - name contains: _time, time_, timestamp, _at (suffix)     → DATETIME
    - name contains: year (standalone)                          → YEAR

  TEXT columns:
    - name contains: description, desc, notes, comment,
                     body, content, details, bio, address       → TEXT
    - name contains: phone, mobile, contact_no, zip, postal    → VARCHAR(20)
    - name contains: email                                      → VARCHAR(150)
    - name contains: url, link, website, image, photo,
                     avatar, file, path                         → VARCHAR(500)
    - name contains: code, ref, reference, sku, serial         → VARCHAR(50)
    - name contains: name, title, label, subject, heading      → VARCHAR(255)
    - name contains: std, sec, class, grade, section           → VARCHAR(10)
    - anything else                                            → VARCHAR(255)

UPSERT KEY RULE:
  - If a column matches the upsert_key field, mark it PRIMARY KEY in CREATE TABLE.
  - If target mode is "create" and a column name ends with _id or is "id",
    make it PRIMARY KEY automatically (first one only).

SQL GENERATION RULES:

1. CREATE TABLE (only if target mode is "create"):
   - Use CREATE TABLE IF NOT EXISTS
   - Apply column type inference rules above to each OUTPUT column
   - If no OUTPUT rows exist: use -- columns inferred at runtime

2. TRUNCATE (only if target mode is "create"):
   - Add TRUNCATE TABLE <target> after CREATE

3. INSERT INTO ... SELECT:
   - SELECT from OUTPUT mappings: src_col AS tgt_col
   - If no OUTPUT rows, use SELECT *
   - FROM first SOURCE table with alias
   - JOIN remaining SOURCE tables using JOIN rows
   - ON clause: combine all JOIN conditions with AND
   - WHERE clause:
       * Each SOURCE row's WHERE condition is wrapped in parentheses
       * Between SOURCE rows, use the operator from column 7 (OR or AND)
       * First SOURCE row has no leading operator
       * Example: WHERE (s.std = '10' OR s.std = '9') OR (c.std = '10') And OR (d.std = '10')

4. UPSERT (if target mode is "upsert"):
   - Add a comment: -- UPSERT on key: <upsert_key>
   - Use INSERT INTO ... SELECT

5. EXISTING (if target mode is "existing"):
   - Skip CREATE and TRUNCATE
   - Only generate INSERT INTO ... SELECT

OUTPUT RULES:
- Output ONLY raw SQL — no markdown, no backticks, no explanation
- Use consistent 2-space indentation
- Add a brief inline comment on each CREATE TABLE column showing why that type was chosen
  Example:  student_id   INT PRIMARY KEY,   -- ID column
            student_name VARCHAR(255),       -- name column
            contact_no   VARCHAR(20),        -- phone/contact column
            maths        DECIMAL(5,2),       -- score/grade column
- If something is ambiguous, make a reasonable assumption and add a SQL comment
"""