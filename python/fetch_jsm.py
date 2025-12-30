import os
import requests
import mysql.connector
from datetime import datetime

# ---------------- CONFIG ----------------
JSM_BASE_URL = os.getenv("JSM_BASE_URL")
JSM_PAT = os.getenv("JSM_PAT")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME")
}

JQL = 'issuetype = Incident AND status not in (Canceled, Cancelled, "Auto Resolved", Resolved)'
MAX_RESULTS = 100

HEADERS = {
    "Authorization": f"Bearer {JSM_PAT}",
    "Accept": "application/json"
}

FIELDS = [
    "issuetype","key","customfield_10130","customfield_10124","status","priority",
    "summary","customfield_10123","assignee","created","resolutiondate","updated",
    "customfield_10131","customfield_10126","customfield_10127","customfield_10132",
    "customfield_10125","customfield_10133","customfield_10134","aggregatetimespent",
    "customfield_10701","customfield_10300","customfield_10801","customfield_10301",
    "customfield_10147","customfield_10145","customfield_10143","customfield_10146",
    "customfield_10148","customfield_10803","customfield_10806","customfield_10804",
    "customfield_10112","customfield_11001","customfield_11403","customfield_11402",
    "customfield_11405","customfield_11404","customfield_11400","customfield_11401",
    "customfield_11406","customfield_11500"
]

# ---------------- HELPERS ----------------

def parse_jira_datetime(value):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f%z") \
            .astimezone(tz=None) \
            .replace(tzinfo=None)
    except Exception:
        return None

def cf_option(val):
    if isinstance(val, dict):
        return val.get("value")
    return None

def cf_user(val):
    if isinstance(val, dict):
        return val.get("displayName")
    return None

# ---------------- FETCH ----------------
def fetch_jsm_issues():
    start_at = 0
    issues = []

    while True:
        params = {
            "jql": JQL,
            "startAt": start_at,
            "maxResults": MAX_RESULTS,
            "fields": ",".join(FIELDS)
        }

        resp = requests.get(
            f"{JSM_BASE_URL}/rest/api/2/search",
            headers=HEADERS,
            params=params,
            timeout=60
        )
        resp.raise_for_status()
        data = resp.json()

        issues.extend(data["issues"])

        if start_at + MAX_RESULTS >= data["total"]:
            break
        start_at += MAX_RESULTS

    return issues

# ---------------- DB LOAD ----------------
def load_to_db(issues):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    try:
        conn.start_transaction()

        cursor.execute("DELETE FROM jsm_open_issues")

        insert_sql = """
        INSERT INTO jsm_open_issues (
          issuekey, issuetype, unit, application, status, priority, summary,
          summary_details, assignee, created, resolved, updated, site_location,
          geography, country, infra_app, affected_ci, issue_category, owner_name,
          time_spent_seconds, assigned_date_bot, escalation_date_l2,
          assigned_date_l2, escalation_date_l3, sop_id, security_incident_comment,
          fault_attribution, closure_code, resolved_by_team, assigned_back_l1,
          assigned_back_l2, last_level_assignee, source, call_summary,
          response_sla, resolution_sla, reason_missed_response_sla,
          reason_missed_resolution_sla, expected_response, expected_resolution,
          services, service_impact, last_refreshed_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        for i in issues:
            f = i["fields"]
            cursor.execute(insert_sql, (
                i["key"],
                f["issuetype"]["name"] if f.get("issuetype") else None,
                cf_option(f.get("customfield_10130")),
                cf_option(f.get("customfield_10124")),
                f["status"]["name"] if f.get("status") else None,
                f["priority"]["name"] if f.get("priority") else None,
                f.get("summary"),
                f.get("customfield_10123"),
                cf_user(f.get("assignee")),
                parse_jira_datetime(f.get("created")),
                parse_jira_datetime(f.get("resolutiondate")),
                parse_jira_datetime(f.get("updated")),
                f.get("customfield_10131"),
                cf_option(f.get("customfield_10126")),
                cf_option(f.get("customfield_10127")),
                cf_option(f.get("customfield_10132")),
                f.get("customfield_10125"),
                cf_option(f.get("customfield_10133")),
                f.get("customfield_10134"),
                f.get("aggregatetimespent"),
                parse_jira_datetime(f.get("customfield_10701")),
                parse_jira_datetime(f.get("customfield_10300"))
                parse_jira_datetime(f.get("customfield_10801")),
                parse_jira_datetime(f.get("customfield_10301")),
                f.get("customfield_10147"),
                f.get("customfield_10145"),
                cf_option(f.get("customfield_10143")),
                cf_option(f.get("customfield_10146")),
                cf_option(f.get("customfield_10148")),
                cf_option(f.get("customfield_10803")),
                cf_option(f.get("customfield_10806")),
                cf_option(f.get("customfield_10804")),
                cf_option(f.get("customfield_10112")),
                f.get("customfield_11001"),
                cf_option(f.get("customfield_11403")),
                cf_option(f.get("customfield_11402")),
                cf_option(f.get("customfield_11405")),
                cf_option(f.get("customfield_11404")),
                parse_jira_datetime(f.get("customfield_11400")),
                parse_jira_datetime(f.get("customfield_11401")),
                cf_option(f.get("customfield_11406")),
                cf_option(f.get("customfield_11500")),
                datetime.utcnow()
            ))

        conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

# ---------------- MAIN ----------------
if __name__ == "__main__":
    issues = fetch_jsm_issues()
    load_to_db(issues)
    print(f"JSM refresh complete: {len(issues)} issues")