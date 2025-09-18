# pgstat_report_disconnect

## Location
src/backend/utils/activity/pgstat_database.c: 208 - 241

## Overview
Reports a database disconnection to PostgreSQL's statistics system, tracking different types of session termination causes.

## Definition
```c
void pgstat_report_disconnect(Oid dboid)
```

## Detailed Description
This function notifies the statistics system when a database connection terminates, categorizing the disconnection by its cause. It examines the global pgStatSessionEndCause variable to determine how the session ended and updates the appropriate counter in the database statistics.

The function distinguishes between different types of disconnections:
- Normal disconnections (not tracked in statistics)
- Client EOF disconnections (counted as abandoned sessions)
- Fatal error disconnections (counted as fatal sessions)
- Killed process disconnections (counted as killed sessions)

Like other connection statistics functions, this only operates for normal backend processes, excluding parallel workers and WAL senders.

## Parameters / Member Variables
- `dboid`: The OID of the database being disconnected from (though the function uses MyDatabaseId for the statistics update)

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_should_report_connstat
  - pgstat_prep_database_pending
  - PgStat_StatDBEntry (data structure)
  - SessionEndType enum (DISCONNECT_NOT_YET, DISCONNECT_NORMAL, DISCONNECT_CLIENT_EOF, DISCONNECT_FATAL, DISCONNECT_KILLED)
  - pgStatSessionEndCause (global variable)
  - MyDatabaseId (global variable)
- Called from (representative examples):
  - pgstat_shutdown_hook (in src/backend/utils/activity/pgstat.c:515)

## Notes and Other Information
- Only reports statistics for normal backend processes (B_BACKEND)
- Normal and not-yet-disconnected sessions are not counted in the statistics
- Client EOF disconnections increment sessions_abandoned counter
- Fatal error disconnections increment sessions_fatal counter  
- Process kill disconnections increment sessions_killed counter
- These statistics help administrators monitor connection stability and identify potential issues
- Uses pending statistics approach for efficient batch updates
- Part of PostgreSQL's database-level session monitoring and health tracking system