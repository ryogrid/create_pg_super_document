# pgstat_report_disconnect

## Location
[src/backend/utils/activity/pgstat_database.c:208-241](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_database.c#L208-L241)

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
  - [pgstat_should_report_connstat](pgstat_should_report_connstat.md)
  - [pgstat_prep_database_pending](pgstat_prep_database_pending.md)
  - [PgStat_StatDBEntry](../P/PgStat_StatDBEntry.md) (data structure)
  - SessionEndType enum (DISCONNECT_NOT_YET, DISCONNECT_NORMAL, DISCONNECT_CLIENT_EOF, DISCONNECT_FATAL, DISCONNECT_KILLED)
  - pgStatSessionEndCause (global variable)
  - MyDatabaseId (global variable)
- Called from (representative examples):
  - [pgstat_shutdown_hook](pgstat_shutdown_hook.md) (in src/backend/utils/activity/pgstat.c:515)

## Notes and Other Information
- Only reports statistics for normal backend processes (B_BACKEND)
- Normal and not-yet-disconnected sessions are not counted in the statistics
- Client EOF disconnections increment sessions_abandoned counter
- Fatal error disconnections increment sessions_fatal counter  
- Process kill disconnections increment sessions_killed counter
- These statistics help administrators monitor connection stability and identify potential issues
- Uses pending statistics approach for efficient batch updates
- Part of PostgreSQL's database-level session monitoring and health tracking system

## Simplified Source

```c
// Simplified version of pgstat_report_disconnect
void pgstat_report_disconnect(Oid dboid) {
    // Skip reporting if connection statistics are disabled
    if (!pgstat_should_report_connstat())
        return;

    // Get database statistics entry for current database
    PgStat_StatDBEntry *dbentry = pgstat_prep_database_pending(MyDatabaseId);

    // Update counters based on how the session ended
    switch (pgStatSessionEndCause) {
        case DISCONNECT_NOT_YET:
        case DISCONNECT_NORMAL:
            // Normal disconnections - no statistics collected
            break;
        case DISCONNECT_CLIENT_EOF:
            dbentry->sessions_abandoned++;
            break;
        case DISCONNECT_FATAL:
            dbentry->sessions_fatal++;
            break;
        case DISCONNECT_KILLED:
            dbentry->sessions_killed++;
            break;
    }
}
```

Key simplifications made:
- Condensed variable declaration and assignment into single line
- Added descriptive comments explaining each major step
- Preserved the exact logic flow and switch statement structure
- Maintained all essential functionality while improving readability
- Clarified that dboid parameter is unused (function uses MyDatabaseId instead)