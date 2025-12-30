import os
import requests
import mysql.connector
from datetime import datetime
from requests.auth import HTTPBasicAuth

# -------------------------------------------------
# Helpers
# -------------------------------------------------
def parse_jira_datetime(value):
    if not value:
        return None
    try:
        return (
            datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f%z")
            .astimezone(tz=None)
            .replace(tzinfo=None)
        )
    except Exception:
        return None


def opt(val):
    return val.get("value") if isinstance(val, dict) else None


def usr(val):
    return val.get("displayName") if isinstance(val, dict) else None


def multi(val):
    if isinstance(val, list):
        return ",".join(
            str(v.get("value") or v.get("displayName") or "")
            for v in val
        )
    return None


# -------------------------------------------------
# Config
# -------------------------------------------------
JIRA_BASE_URL = os.getenv("JIRA_BASE_URL")
JIRA_USER = os.getenv("JIRA_USER")
JIRA_PASS = os.getenv("JIRA_PASS")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

JQL = "filter = NOC-6"
MAX_RESULTS = 100

# -------------------------------------------------
# Fetch JIRA Issues
# -------------------------------------------------
def fetch_jira_issues():
    issues = []
    start_at = 0

    while True:
        params = {
            "jql": JQL,
            "startAt": start_at,
            "maxResults": MAX_RESULTS,
        }

        resp = requests.get(
            f"{JIRA_BASE_URL}/rest/api/2/search",
            auth=HTTPBasicAuth(JIRA_USER, JIRA_PASS),
            params=params,
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()

        issues.extend(data.get("issues", []))

        if start_at + MAX_RESULTS >= data.get("total", 0):
            break
        start_at += MAX_RESULTS

    return issues


# -------------------------------------------------
# Load into MySQL
# -------------------------------------------------
def load_to_db(issues):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    insert_sql = """
    INSERT INTO jira_open_issues (
        issuekey, summary, status, assignee, reporter,
        created, updated, issuetype, brief_description,
        incident_source, country, unit, affected_ci, infra_app,
        owner_name, incident_geography, application_name,
        incident_priority, incident_assigned_to, site_location,
        call_summary, jsm_key, response_sla, resolution_sla,
        services, category, security_incident, comments,
        fault_attribution, closure_code, resolved_by,
        reason_missed_resolution_sla, resources,
        resolution_completion_date, task_type, task_sub_type,
        request_type, product_variants, customers, priority,
        bug_type, resolution_details, bug_reason,
        response_sla_bug, resolution_sla_bug, reported_by,
        resolved, rca, capa, known_issue, five_why,
        validator_approved, last_refreshed_at
    )
    VALUES (
        %s,%s,%s,%s,%s,
        %s,%s,%s,%s,
        %s,%s,%s,%s,%s,
        %s,%s,%s,
        %s,%s,%s,
        %s,%s,%s,%s,
        %s,%s,%s,%s,
        %s,%s,%s,
        %s,%s,
        %s,%s,%s,
        %s,%s,%s,
        %s,%s,%s,
        %s,%s,%s,
        %s,%s,%s,%s,
        %s,%s,%s
    )
    """

    try:
        conn.start_transaction()
        cursor.execute("DELETE FROM jira_open_issues")

        for issue in issues:
            f = issue["fields"]

            values = (
                issue["key"],
                f.get("summary"),
                f["status"]["name"] if f.get("status") else None,
                usr(f.get("assignee")),
                usr(f.get("reporter")),

                parse_jira_datetime(f.get("created")),
                parse_jira_datetime(f.get("updated")),
                f["issuetype"]["name"] if f.get("issuetype") else None,
                f.get("customfield_23866"),

                opt(f.get("customfield_14267")),
                opt(f.get("customfield_11266")),
                opt(f.get("customfield_15570")),
                f.get("customfield_15262"),
                opt(f.get("customfield_13861")),

                f.get("customfield_15578"),
                opt(f.get("customfield_15560")),
                opt(f.get("customfield_15960")),

                opt(f.get("customfield_14261")),
                opt(f.get("customfield_13061")),
                f.get("customfield_15964"),

                f.get("customfield_15579"),
                f.get("customfield_15574"),
                opt(f.get("customfield_21184")),
                opt(f.get("customfield_21185")),

                multi(f.get("customfield_25561")),
                opt(f.get("customfield_10694")),
                opt(f.get("customfield_27870")),
                f.get("customfield_10041"),

                opt(f.get("customfield_23979")),
                opt(f.get("customfield_15565")),
                opt(f.get("customfield_22361")),

                opt(f.get("customfield_22716")),
                multi(f.get("customfield_10748")),

                parse_jira_datetime(f.get("customfield_10076")),
                opt(f.get("customfield_10190")),
                opt(f.get("customfield_23875")),

                opt(f.get("customfield_10007")),
                multi(f.get("customfield_10078")),
                multi(f.get("customfield_10001")),
                f["priority"]["name"] if f.get("priority") else None,

                opt(f.get("customfield_21460")),
                f.get("customfield_10077"),
                opt(f.get("customfield_15060")),

                f.get("customfield_21161"),
                f.get("customfield_21160"),
                usr(f.get("customfield_20760")),

                parse_jira_datetime(f.get("resolutiondate")),
                f.get("customfield_10850"),
                f.get("customfield_10851"),
                opt(f.get("customfield_29660")),
                f.get("customfield_15162"),
                opt(f.get("customfield_29662")),

                datetime.utcnow(),
            )

            # Safety check to prevent SQL mismatch
            if len(values) != insert_sql.count("%s"):
                raise Exception(
                    f"SQL mismatch: {len(values)} values for "
                    f"{insert_sql.count('%s')} placeholders"
                )

            cursor.execute(insert_sql, values)

        conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


# -------------------------------------------------
# Main
# -------------------------------------------------
if __name__ == "__main__":
    issues = fetch_jira_issues()
    load_to_db(issues)
    print(f"JIRA refresh complete: {len(issues)} issues")