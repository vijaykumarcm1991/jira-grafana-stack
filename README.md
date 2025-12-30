## Jira → MySQL → Grafana Stack

### Start the stack
docker compose up -d --build

### Access Grafana
http://localhost:3000  
Default login: admin / admin

### Add MySQL Data Source
Host: mysql:3306  
Database: jira_db  
User: root  
Password: root

### Tables
- jsm_open_issues
- jira_open_issues

### Refresh Interval
Every 5 minutes