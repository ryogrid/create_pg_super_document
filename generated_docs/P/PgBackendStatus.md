# PgBackendStatus

## Location
src/include/utils/backend_status.h: 97 - 173

## Overview
PgBackendStatus is the core shared-memory structure that maintains comprehensive runtime information about each PostgreSQL backend process, including connection details, current activity, and progress reporting data.

## Definition
```c
typedef struct PgBackendStatus
{
    /*
     * To avoid locking overhead, we use the following protocol: a backend
     * increments st_changecount before modifying its entry, and again after
     * finishing a modification.  A would-be reader should note the value of
     * st_changecount, copy the entry into private memory, then check
     * st_changecount again.  If the value hasn't changed, and if it's even,
     * the copy is valid; otherwise start over.
     */
    int             st_changecount;

    /* The entry is valid iff st_procpid > 0, unused if st_procpid == 0 */
    int             st_procpid;

    /* Type of backends */
    BackendType     st_backendType;

    /* Times when current backend, transaction, and activity started */
    TimestampTz     st_proc_start_timestamp;
    TimestampTz     st_xact_start_timestamp;
    TimestampTz     st_activity_start_timestamp;
    TimestampTz     st_state_start_timestamp;

    /* Database OID, owning user's OID, connection client address */
    Oid             st_databaseid;
    Oid             st_userid;
    SockAddr        st_clientaddr;
    char           *st_clienthostname;      /* MUST be null-terminated */

    /* Information about SSL connection */
    bool            st_ssl;
    PgBackendSSLStatus *st_sslstatus;

    /* Information about GSSAPI connection */
    bool            st_gss;
    PgBackendGSSStatus *st_gssstatus;

    /* current state */
    BackendState    st_state;

    /* application name; MUST be null-terminated */
    char           *st_appname;

    /* Current command string; MUST be null-terminated */
    char           *st_activity_raw;

    /* Command progress reporting */
    ProgressCommandType st_progress_command;
    Oid             st_progress_command_target;
    int64           st_progress_param[PGSTAT_NUM_PROGRESS_PARAM];

    /* query identifier, optionally computed using post_parse_analyze_hook */
    uint64          st_query_id;
} PgBackendStatus;
```

## Detailed Description
PgBackendStatus is the central data structure for PostgreSQL's backend activity monitoring system. Each live backend process (including auxiliary processes) maintains one of these structures in shared memory, providing real-time visibility into database server activity.

The structure uses a lock-free protocol based on st_changecount to avoid contention between writers (backends updating their status) and readers (monitoring queries). This design prioritizes fast updates over read performance, which is appropriate since status updates are much more frequent than status reads.

The structure tracks multiple categories of information: process identification, timing data, connection details (including SSL/GSS status), current activity, and command progress. This comprehensive view enables detailed monitoring through system views like pg_stat_activity.

## Parameters / Member Variables
- `st_changecount`: Lock-free synchronization counter for safe concurrent access without locking
- `st_procpid`: Process ID of the backend (> 0 indicates valid entry, 0 means unused slot)
- `st_backendType`: Type of backend process (regular, autovacuum, background worker, etc.)
- `st_proc_start_timestamp`: When the backend process started
- `st_xact_start_timestamp`: When the current transaction started (NULL if no active transaction)
- `st_activity_start_timestamp`: When the current activity/query started
- `st_state_start_timestamp`: When the current state was entered
- `st_databaseid`: OID of the database this backend is connected to
- `st_userid`: OID of the user this backend is running as
- `st_clientaddr`: Network address of the client connection
- `st_clienthostname`: Hostname of the client (null-terminated)
- `st_ssl`: Boolean flag indicating if SSL is enabled for this connection
- `st_sslstatus`: Pointer to detailed SSL status information (if st_ssl is true)
- `st_gss`: Boolean flag indicating if GSSAPI is enabled for this connection
- `st_gssstatus`: Pointer to detailed GSS status information (if st_gss is true)
- `st_state`: Current backend state (idle, active, idle in transaction, etc.)
- `st_appname`: Application name provided by the client (null-terminated)
- `st_activity_raw`: Current SQL command or activity description (null-terminated, may be truncated)
- `st_progress_command`: Type of command currently reporting progress
- `st_progress_command_target`: OID of the relation targeted by the progress-reporting command
- `st_progress_param[]`: Array of command-specific progress parameters
- `st_query_id`: Optional query identifier for query tracking and analysis

## Dependencies
- Types referenced:
  - BackendType (backend process classification)
  - SockAddr (network address structure)
  - PgBackendSSLStatus (SSL connection details)
  - PgBackendGSSStatus (GSS connection details)
  - BackendState (backend activity state)
  - ProgressCommandType (progress reporting command types)
- Constants referenced:
  - PGSTAT_NUM_PROGRESS_PARAM (size of progress parameter array)
- Used by:
  - Progress reporting functions (pgstat_progress_*)
  - Backend status management functions (pgstat_bestart, pgstat_report_*, etc.)
  - System statistics functions (pg_stat_get_backend_*)
  - LocalPgBackendStatus (as base structure)

## Notes and Other Information
- Allocated per ProcNumber in shared memory but ProcNumber assignment is not critical to functionality
- Uses lock-free protocol with memory barriers for high-performance concurrent access
- The st_activity_raw field may contain truncated multi-byte characters; use pgstat_clip_activity() for proper display
- SSL and GSS status pointers are only valid when corresponding boolean flags are true
- Progress reporting is optional and command-specific - not all commands populate progress fields
- Query ID computation depends on post_parse_analyze_hook configuration
- Accessible through various system views including pg_stat_activity, pg_stat_progress_*, pg_stat_ssl, and pg_stat_gssapi
- Unrelated to the cumulative statistics system (pgstat.c) - this is for real-time activity monitoring only