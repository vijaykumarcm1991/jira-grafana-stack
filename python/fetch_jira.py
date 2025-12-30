import os
import requests
import mysql.connector
from datetime import datetime
from requests.auth import HTTPBasicAuth

# ---------------- CONFIG ----------------
JIRA_BASE_URL = os.getenv("JIRA_BASE_URL")
JIRA_USER = os.getenv("JIRA_USER")
JIRA_PASS = os.getenv("JIRA_PASS")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME")
}

JQL = "filter = NOC-6"
MAX_RESULTS = 100

FIELDS = [
    "key","summary","status","assignee","reporter","created","updated",
    "issuetype","customfield_23866","customfield_14267","customfield_11266",
    "customfield_15570","customfield_15262","customfield_13861",
    "customfield_15578","customfield_15560","customfield_15960",
    "customfield_14261","customfield_13061","customfield_15964",
    "customfield_15579","customfield_15574","customfield_21184",
    "customfield_21185","customfield_25561","customfield_10694",
    "customfield_27870","customfield_10041","customfield_23979",
    "customfield_15565","customfield_22361","customfield_22716",
    "customfield_10748","customfield_10076","customfield_10190",
    "customfield_23875","customfield_10007","customfield_10078",
    "customfield_10001","priority","customfield_21460","customfield_10077",
    "customfield_15060","customfield_21161","customfield_21160",
    "customfield_20760","resolutiondate","customfield_10850",
    "customfield_10851","customfield_29660","customfield_15162",
    "customfield_29662"
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

def option(val):
    if isinstance(val, dict):
        return val.get("value")
    return None

def user(val):
    if isinstance(val, dict):
        return val.get("displayName")
    return None

def multi(val):
    if isinstance(val, list):
        return ",".join([v.get("value") or v.get("displayName") for v in val])
    return None

# ---------------- FETCH ----------------
def fetch_jira_issues():
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
            f"{JIRA_BASE_URL}/rest/api/2/search",
            auth=HTTPBasicAuth(JIRA_USER, JIRA_PASS),
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
        cursor.execute("DELETE FROM jira_open_issues")

        insert_sql = """
        INSERT INTO jira_open_issues (
          issuekey, summary, status, assignee, reporter, created, updated,
          issuetype, brief_description, incident_source, country, unit,
          affected_ci, infra_app, owner_name, incident_geography,
          application_name, incident_priority, incident_assigned_to,
          site_location, call_summary, jsm_key, response_sla, resolution_sla,
          services, category, security_incident, comments, fault_attribution,
          closure_code, resolved_by, reason_missed_resolution_sla, resources,
          resolution_completion_date, task_type, task_sub_type, request_type,
          product_variants, customers, priority, bug_type, resolution_details,
          bug_reason, response_sla_bug, resolution_sla_bug, reported_by,
          resolved, rca, capa, known_issue, five_why, validator_approved,
          last_refreshed_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s)
        """
        
        for i in issues:
            f = i["fields"]
            cursor.execute(insert_sql, (
                i["key"], f.get("summary"),
                f["status"]["name"] if f.get("status") else None,
                user(f.get("assignee")), user(f.get("reporter")),
                parse_jira_datetime(f.get("created")),
                parse_jira_datetime(f.get("updated")),
                f["issuetype"]["name"] if f.get("issuetype") else None,
                f.get("customfield_23866"),
                option(f.get("customfield_14267")),
                option(f.get("customfield_11266")),
                option(f.get("customfield_15570")),
                f.get("customfield_15262"),
                option(f.get("customfield_13861")),
                f.get("customfield_15578"),
                option(f.get("customfield_15560")),
                option(f.get("customfield_15960")),
                option(f.get("customfield_14261")),
                option(f.get("customfield_13061")),
                f.get("customfield_15964"),
                f.get("customfield_15579"),
                f.get("customfield_15574"),
                option(f.get("customfield_21184")),
                option(f.get("customfield_21185")),
                multi(f.get("customfield_25561")),
                option(f.get("customfield_10694")),
                option(f.get("customfield_27870")),
                f.get("customfield_10041"),
                option(f.get("customfield_23979")),
                option(f.get("customfield_15565")),
                option(f.get("customfield_22361")),
                option(f.get("customfield_22716")),
                multi(f.get("customfield_10748")),
                parse_jira_datetime(f.get("customfield_10076")),
                option(f.get("customfield_10190")),
                option(f.get("customfield_23875")),
                option(f.get("customfield_10007")),
                multi(f.get("customfield_10078")),
                multi(f.get("customfield_10001")),
                f["priority"]["name"] if f.get("priority") else None,
                option(f.get("customfield_21460")),
                f.get("customfield_10077"),
                option(f.get("customfield_15060")),
                f.get("customfield_21161"),
                f.get("customfield_21160"),
                user(f.get("customfield_20760")),
                parse_jira_datetime(f.get("resolutiondate")),
                f.get("customfield_10850"),
                f.get("customfield_10851"),
                option(f.get("customfield_29660")),
                f.get("customfield_15162"),
                option(f.get("customfield_29662")),
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
    issues = fetch_jira_issues()
    load_to_db(issues)
    print(f"JIRA refresh complete: {len(issues)} issues")