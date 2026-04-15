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
JSM_USER = os.getenv("JSM_USER")        # email
JSM_API_TOKEN = os.getenv("JSM_API_TOKEN")

AUTH = (JSM_USER, JSM_API_TOKEN)

JQL = 'project = NDCCLOUD'
MAX_RESULTS = 500

# ---------------- FETCH ----------------
def fetch_jsm_issues():
    issues = []
    next_token = None
    page = 1

    while True:
        params = {
            "jql": JQL,
            "maxResults": 50,
            "fields": "*all"
        }

        if next_token:
            params["nextPageToken"] = next_token

        print("\n==============================")
        print(f"[PAGE {page}] Calling API...")
        print("PARAMS:", params)
        print("==============================")

        r = requests.get(
            f"{JSM_BASE_URL}/rest/api/3/search/jql",
            auth=AUTH,
            params=params,
            timeout=60
        )

        print("STATUS CODE:", r.status_code)

        if r.status_code != 200:
            print("ERROR RESPONSE:", r.text)
            r.raise_for_status()

        data = r.json()

        batch = data.get("issues", [])
        issues.extend(batch)

        print("ISSUES THIS PAGE:", len(batch))
        print("TOTAL COLLECTED:", len(issues))

        if batch:
            print("SAMPLE ISSUE:", batch[0].get("key"))

        next_token = data.get("nextPageToken")

        print("NEXT TOKEN:", next_token)

        if not next_token:
            print("No more pages. Done.")
            break

        page += 1

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
            opt(f.get("customfield_10086")),
            opt(f.get("customfield_10085")),
            f["status"]["name"] if f.get("status") else None,
            f["priority"]["name"] if f.get("priority") else None,
            f.get("summary"),
            f.get("customfield_10093"),
            usr(f.get("assignee")),
            parse_jira_datetime(f.get("created")),
            parse_jira_datetime(f.get("resolutiondate")),
            parse_jira_datetime(f.get("updated")),
            f.get("customfield_10092"),
            opt(f.get("customfield_10097")),
            opt(f.get("customfield_10091")),
            opt(f.get("customfield_10099")),
            f.get("customfield_10094"),
            opt(f.get("customfield_10096")),
            f.get("customfield_10098"),
            f.get("aggregatetimespent"),
            parse_jira_datetime(f.get("customfield_10142")),
            parse_jira_datetime(f.get("customfield_10130")),
            parse_jira_datetime(f.get("customfield_10144")),
            parse_jira_datetime(f.get("customfield_10146")),
            f.get("customfield_10157"),
            f.get("customfield_10156"),
            opt(f.get("customfield_10105")),
            opt(f.get("customfield_10103")),
            opt(f.get("customfield_10104")),
            opt(f.get("customfield_10145")),
            opt(f.get("customfield_10162")),
            opt(f.get("customfield_10138")),
            opt(f.get("customfield_10095")),
            f.get("customfield_10102"),
            opt(f.get("customfield_10108")),
            opt(f.get("customfield_10106")),
            parse_jira_datetime(f.get("customfield_10114")),
            parse_jira_datetime(f.get("customfield_10115")),
            opt(f.get("customfield_10113")),
            opt(f.get("customfield_10118")),
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