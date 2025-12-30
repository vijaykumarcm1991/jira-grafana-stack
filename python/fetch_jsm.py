import os
import requests
import mysql.connector
from datetime import datetime

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


# -------------------------------------------------
# Config
# -------------------------------------------------
JSM_BASE_URL = os.getenv("JSM_BASE_URL")
JSM_PAT = os.getenv("JSM_PAT")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

HEADERS = {
    "Authorization": f"Bearer {JSM_PAT}",
    "Accept": "application/json",
}

JQL = 'issuetype = Incident AND status not in (Canceled, Cancelled, "Auto Resolved", Resolved)'
MAX_RESULTS = 100

# -------------------------------------------------
# Fetch JSM Issues
# -------------------------------------------------
def fetch_jsm_issues():
    issues = []
    start_at = 0

    while True:
        params = {
            "jql": JQL,
            "startAt": start_at,
            "maxResults": MAX_RESULTS,
        }

        resp = requests.get(
            f"{JSM_BASE_URL}/rest/api/2/search",
            headers=HEADERS,
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
    INSERT INTO jsm_open_issues (
        issuekey, issuetype, unit, application, status, priority,
        summary, summary_details, assignee,
        created, resolved, updated,
        site_location, geography, country, infra_app, affected_ci,
        issue_category, owner_name, time_spent_seconds,
        assigned_date_bot, escalation_date_l2, assigned_date_l2, escalation_date_l3,
        sop_id, security_incident_comment,
        fault_attribution, closure_code, resolved_by_team,
        assigned_back_l1, assigned_back_l2, last_level_assignee,
        source, call_summary,
        response_sla, resolution_sla,
        reason_missed_response_sla, reason_missed_resolution_sla,
        expected_response, expected_resolution,
        services, service_impact,
        last_refreshed_at
    )
    VALUES (
        %s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,
        %s,%s,%s,
        %s,%s,
        %s,%s,
        %s,%s,
        %s,%s,
        %s,%s
    )
    """

    try:
        conn.start_transaction()
        cursor.execute("DELETE FROM jsm_open_issues")

        for issue in issues:
            f = issue["fields"]

            values = (
                issue["key"],
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

                opt(f.get("customfield_11405")),
                opt(f.get("customfield_11404")),

                parse_jira_datetime(f.get("customfield_11400")),
                parse_jira_datetime(f.get("customfield_11401")),

                opt(f.get("customfield_11406")),
                opt(f.get("customfield_11500")),

                datetime.utcnow(),
            )

            # Safety check
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
    issues = fetch_jsm_issues()
    load_to_db(issues)
    print(f"JSM refresh complete: {len(issues)} issues")