# setup_connection

## Location
[src/bin/pg_dump/pg_dump.c:1212-1381](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L1212-L1381)

## Overview
Establishes and configures a database connection for pg_dump operations, setting up appropriate encoding, transaction isolation, timeouts, and other parameters needed for consistent data dumping.

## Definition

```c
static void
setup_connection(Archive *AH, const char *dumpencoding,
				 const char *dumpsnapshot, char *use_role)
```
## Detailed Description
The setup_connection function performs comprehensive initialization of a database connection specifically for pg_dump operations. It configures various PostgreSQL settings to ensure consistent, portable, and secure data dumping. The function sets client encoding, establishes proper transaction isolation levels, configures timeouts, and handles snapshot synchronization for parallel dumps. It also applies security measures by restricting access to certain relation types and setting up role-based access if specified.

The function handles both single-process and parallel dump scenarios, with special logic for coordinating snapshots across multiple worker processes. It sets various PostgreSQL parameters to ensure the dump output is deterministic and portable across different PostgreSQL installations.

## Parameters / Member Variables
- `*AH`: Archive handle containing connection information and dump configuration
- `*dumpencoding`: Client encoding to use for the connection (can be NULL)
- `*dumpsnapshot`: Specific snapshot ID to use for consistent reads (can be NULL)
- `*use_role`: Database role to assume for the dump operation (can be NULL)
## Dependencies
- Functions called/Symbols referenced:
  - [GetConnection](../G/GetConnection.md) (retrieve database connection from archive)
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md) (execute SQL returning single row)
  - [ExecuteSqlStatement](../E/ExecuteSqlStatement.md) (execute SQL statement without return)
  - [PQsetClientEncoding](../P/PQsetClientEncoding.md) (set client character encoding)
  - [PQclientEncoding](../P/PQclientEncoding.md) (get current client encoding)
  - [setFmtEncoding](setFmtEncoding.md) (set encoding for formatting functions)
  - [PQparameterStatus](../P/PQparameterStatus.md) (get server parameter value)
  - [fmtId](../f/fmtId.md) (format identifier with proper quoting)
  - [set_restrict_relation_kind](set_restrict_relation_kind.md) (restrict access to relation types)
  - [get_synchronized_snapshot](../g/get_synchronized_snapshot.md) (obtain synchronized snapshot for parallel dumps)
  - [pg_malloc0](../p/pg_malloc0.md) (allocate zero-initialized memory)
  - [pg_strdup](../p/pg_strdup.md) (duplicate string)
- Called from (representative examples):
  - [main](../m/main.md) (primary setup for main dump process)
  - [setupDumpWorker](setupDumpWorker.md) (setup for parallel dump worker processes)

## Notes and Other Information
- Function is marked static, limiting scope to pg_dump.c file
- Sets search_path to secure default to prevent malicious function calls
- Configures DATESTYLE to ISO and INTERVALSTYLE to POSTGRES for portability
- Sets extra_float_digits for exact floating-point representation
- Disables synchronized sequential scans for consistent ordering
- Sets various timeout parameters to 0 to prevent interruption
- Handles version-specific features based on remote server version
- Supports row-level security configuration for PostgreSQL 9.5+
- Initializes prepared query tracking state for the connection
- Establishes transaction with appropriate isolation level (SERIALIZABLE or REPEATABLE READ)
- Handles snapshot coordination for parallel dumps through snapshot IDs
- Includes special handling for standby servers in parallel dump scenarios

## Simplified Source

```c
static void setup_connection(Archive *AH, const char *dumpencoding,
                            const char *dumpsnapshot, char *use_role) {
    DumpOptions *dopt = AH->dopt;
    PGconn *conn = GetConnection(AH);

    // Set secure search path
    PQclear(ExecuteSqlQueryForSingleRow(AH, ALWAYS_SECURE_SEARCH_PATH_SQL));

    // Configure client encoding if specified
    if (dumpencoding) {
        if (PQsetClientEncoding(conn, dumpencoding) < 0)
            pg_fatal("invalid client encoding \"%s\" specified", dumpencoding);
    }

    // Store encoding settings for string escaping
    AH->encoding = PQclientEncoding(conn);
    setFmtEncoding(AH->encoding);

    const char *std_strings = PQparameterStatus(conn, "standard_conforming_strings");
    AH->std_strings = (std_strings && strcmp(std_strings, "on") == 0);

    // Set database role if specified
    if (!use_role && AH->use_role)
        use_role = AH->use_role;

    if (use_role) {
        PQExpBuffer query = createPQExpBuffer();
        appendPQExpBuffer(query, "SET ROLE %s", fmtId(use_role));
        ExecuteSqlStatement(AH, query->data);
        destroyPQExpBuffer(query);

        if (!AH->use_role)
            AH->use_role = pg_strdup(use_role);
    }

    // Configure PostgreSQL settings for consistent dumps
    ExecuteSqlStatement(AH, "SET DATESTYLE = ISO");
    ExecuteSqlStatement(AH, "SET INTERVALSTYLE = POSTGRES");

    // Set floating-point precision
    if (have_extra_float_digits) {
        PQExpBuffer q = createPQExpBuffer();
        appendPQExpBuffer(q, "SET extra_float_digits TO %d", extra_float_digits);
        ExecuteSqlStatement(AH, q->data);
        destroyPQExpBuffer(q);
    } else {
        ExecuteSqlStatement(AH, "SET extra_float_digits TO 3");
    }

    // Configure for consistent behavior
    ExecuteSqlStatement(AH, "SET synchronize_seqscans TO off");

    // Disable timeouts
    ExecuteSqlStatement(AH, "SET statement_timeout = 0");
    if (AH->remoteVersion >= 90300)
        ExecuteSqlStatement(AH, "SET lock_timeout = 0");
    if (AH->remoteVersion >= 90600)
        ExecuteSqlStatement(AH, "SET idle_in_transaction_session_timeout = 0");
    if (AH->remoteVersion >= 170000)
        ExecuteSqlStatement(AH, "SET transaction_timeout = 0");

    // Additional configuration
    if (quote_all_identifiers)
        ExecuteSqlStatement(AH, "SET quote_all_identifiers = true");

    // Row-level security (PostgreSQL 9.5+)
    if (AH->remoteVersion >= 90500) {
        if (dopt->enable_row_security)
            ExecuteSqlStatement(AH, "SET row_security = on");
        else
            ExecuteSqlStatement(AH, "SET row_security = off");
    }

    // Security restrictions
    set_restrict_relation_kind(AH, "view, foreign-table");

    // Initialize prepared query state
    AH->is_prepared = (bool *) pg_malloc0(NUM_PREP_QUERIES * sizeof(bool));

    // Start transaction with appropriate isolation level
    ExecuteSqlStatement(AH, "BEGIN");

    if (dopt->serializable_deferrable && AH->sync_snapshot_id == NULL)
        ExecuteSqlStatement(AH, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE");
    else
        ExecuteSqlStatement(AH, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY");

    // Handle snapshot for consistent reads
    if (dumpsnapshot)
        AH->sync_snapshot_id = pg_strdup(dumpsnapshot);

    if (AH->sync_snapshot_id) {
        PQExpBuffer query = createPQExpBuffer();
        appendPQExpBufferStr(query, "SET TRANSACTION SNAPSHOT ");
        appendStringLiteralConn(query, AH->sync_snapshot_id, conn);
        ExecuteSqlStatement(AH, query->data);
        destroyPQExpBuffer(query);
    } else if (AH->numWorkers > 1) {
        // Get synchronized snapshot for parallel dumps
        if (AH->isStandby && AH->remoteVersion < 100000)
            pg_fatal("parallel dumps from standby servers are not supported by this server version");
        AH->sync_snapshot_id = get_synchronized_snapshot(AH);
    }
}
```