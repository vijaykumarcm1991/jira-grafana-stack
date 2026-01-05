import os
import time
import requests
import mysql.connector
from datetime import datetime

# ---------------- DB RETRY ----------------
def get_db_connection(cfg, retries=30, delay=5):
    for i in range(retries):
        try:
            return mysql.connector.connect(**cfg)
        except mysql.connector.Error as e:
            print(f"MySQL not ready ({i+1}/{retries}) - {e}")
            time.sleep(delay)
    raise RuntimeError("MySQL never became ready")

# ---------------- HELPERS ----------------
def parse_jira_datetime(v):
    if not v:
        return None
    try:
        return (
            datetime.strptime(v, "%Y-%m-%dT%H:%M:%S.%f%z")
            .replace(tzinfo=None)
        )
    except Exception:
        return None

def opt(v):
    return v.get("value") if isinstance(v, dict) else None

def usr(v):
    return v.get("displayName") if isinstance(v, dict) else None

# ---------------- CONFIG ----------------
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

JSM_BASE_URL = os.getenv("JSM_BASE_URL")
JSM_PAT = os.getenv("JSM_PAT")

HEADERS = {
    "Authorization": f"Bearer {JSM_PAT}",
    "Accept": "application/json"
}

JQL = 'issuetype = Incident AND status not in (Canceled, Cancelled, "Auto Resolved", Resolved)'
MAX_RESULTS = 500

# ---------------- FETCH ----------------
def fetch_jsm_issues():
    issues, start = [], 0
    while True:
        r = requests.get(
            f"{JSM_BASE_URL}/rest/api/2/search",
            headers=HEADERS,
            params={"jql": JQL, "startAt": start, "maxResults": MAX_RESULTS},
            timeout=60
        )
        r.raise_for_status()
        data = r.json()
        issues.extend(data["issues"])
        if start + MAX_RESULTS >= data["total"]:
            break
        start += MAX_RESULTS
    return issues

# ---------------- LOAD ----------------
def load_to_db(issues):
    conn = get_db_connection(DB_CONFIG)
    cur = conn.cursor()

    columns = [
        "issuekey","issuetype","unit","application","status","priority",
        "summary","summary_details","assignee",
        "created","resolved","updated",
        "site_location","geography","country","infra_app","affected_ci",
        "issue_category","owner_name","time_spent_seconds",
        "assigned_date_bot","escalation_date_l2","assigned_date_l2",
        "escalation_date_l3","sop_id","security_incident_comment",
        "fault_attribution","closure_code","resolved_by_team",
        "assigned_back_l1","assigned_back_l2","last_level_assignee",
        "source","call_summary","response_sla","resolution_sla",
        "expected_response","expected_resolution",
        "services","service_impact","last_refreshed_at"
    ]

    placeholders = ",".join(["%s"] * len(columns))
    sql = f"INSERT INTO jsm_open_issues ({','.join(columns)}) VALUES ({placeholders})"

    conn.start_transaction()
    cur.execute("DELETE FROM jsm_open_issues")

    for i in issues:
        f = i["fields"]
        values = (
            i["key"],
            f["issuetype"]["name"] if f.get("issuetype") else None,
            opt(f.get("customfield_10130")),
            opt(f.get("customfield_10124")),
            f["status"]["name"] if f.get("status") else None,
            f["priority"]["name"] if f.get("priority") else None,
            f.get("summary"),
            f.get("customfield_10123"),
            usr(f.get("assignee")),
            parse_jira_datetime(f.get("created")),
            parse_jira_datetime(f.get("resolutiondate")),
            parse_jira_datetime(f.get("updated")),
            f.get("customfield_10131"),
            opt(f.get("customfield_10126")),
            opt(f.get("customfield_10127")),
            opt(f.get("customfield_10132")),
            f.get("customfield_10125"),
            opt(f.get("customfield_10133")),
            f.get("customfield_10134"),
            f.get("aggregatetimespent"),
            parse_jira_datetime(f.get("customfield_10701")),
            parse_jira_datetime(f.get("customfield_10300")),
            parse_jira_datetime(f.get("customfield_10801")),
            parse_jira_datetime(f.get("customfield_10301")),
            f.get("customfield_10147"),
            f.get("customfield_10145"),
            opt(f.get("customfield_10143")),
            opt(f.get("customfield_10146")),
            opt(f.get("customfield_10148")),
            opt(f.get("customfield_10803")),
            opt(f.get("customfield_10806")),
            opt(f.get("customfield_10804")),
            opt(f.get("customfield_10112")),
            f.get("customfield_11001"),
            opt(f.get("customfield_11403")),
            opt(f.get("customfield_11402")),
            parse_jira_datetime(f.get("customfield_11400")),
            parse_jira_datetime(f.get("customfield_11401")),
            opt(f.get("customfield_11406")),
            opt(f.get("customfield_11500")),
            datetime.utcnow()
        )

        if len(values) != len(columns):
            raise Exception(f"JSM mismatch: {len(values)} vs {len(columns)}")

        cur.execute(sql, values)

    conn.commit()
    cur.close()
    conn.close()

# ---------------- MAIN ----------------
if __name__ == "__main__":
    issues = fetch_jsm_issues()
    load_to_db(issues)
    print(f"JSM refresh complete: {len(issues)}")