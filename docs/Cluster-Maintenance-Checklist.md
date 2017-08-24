## Checklist, and Alarms to Create
1. Check alarm summary.
  - Create an alarm summary for #opsdx-alerts report #alarm, #ok, #insufficient over the last 6/12/24 hours
2. Check for pods with errors on dev + prod cluster.
  - Pod errors on recurring jobs.
3. Check for node resources, e.g., space, CPU usage, memory.
  - Node disk free/used alarm
  - RDS free/used alarm
4. Check for service performance, e.g., latency.
5. Check for security and audit log.
6. Check for data quality.
7. Check for pending issues.

## Alarm
From now on we will tag every alarm (i.e., in the [ALARM] state) with a emoji reaction for:

:bangbang: = flag alarm for analysis (not started)

:zap: = minor alarm (resolution in progress)

:fire: = major alarm (resolution in progress)

:thumbsup: or :white_check_mark: = alarm resolved

:thumbsdown:  = false alarm (don't worry)