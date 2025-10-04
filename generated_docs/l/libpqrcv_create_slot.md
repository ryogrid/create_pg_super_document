# libpqrcv_create_slot

## Location
[src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:1010-1122](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/libpqwalreceiver/libpqwalreceiver.c#L1010-L1122)

## Overview
libpqrcv_create_slot creates a new replication slot on the primary server, handling both logical and physical slots with various configuration options and returning the exported snapshot name for logical slots.

## Definition
```c
static char *libpqrcv_create_slot(WalReceiverConn *conn, const char *slotname,
                                 bool temporary, bool two_phase, bool failover,
                                 CRSSnapshotAction snapshot_action, XLogRecPtr *lsn)
```

## Detailed Description
This function constructs and executes a CREATE_REPLICATION_SLOT command on the primary server through the streaming replication connection. It dynamically builds the SQL command based on the slot type (logical vs physical) and various options. For logical slots, it uses the 'pgoutput' plugin and supports advanced features like two-phase commit and failover capabilities. The function adapts its syntax based on the server version (15.0+ uses new parenthesized options syntax). For physical slots, it includes the RESERVE_WAL option to prevent WAL cleanup. Upon successful creation, it extracts the LSN and snapshot information from the result, returning the snapshot name for logical slots or NULL for physical slots.

## Parameters / Member Variables
- `conn`: Pointer to WalReceiverConn structure containing the streaming connection to the primary server
- `slotname`: Name of the replication slot to create (must be properly quoted in the command)
- `temporary`: Boolean flag to create a temporary slot that will be automatically dropped on disconnection
- `two_phase`: Boolean flag to enable two-phase commit support for logical replication
- `failover`: Boolean flag to mark the slot as eligible for failover to standby servers
- `snapshot_action`: Enum specifying snapshot handling (export, no export, or use existing)
- `lsn`: Output parameter to receive the slot's consistent LSN position (can be NULL if not needed)

## Dependencies
- Functions called/Symbols referenced:
  - [PQserverVersion](../P/PQserverVersion.md): libpq function to get the server version for syntax compatibility
  - [initStringInfo](../i/initStringInfo.md)/appendStringInfo: PostgreSQL string manipulation functions
  - [libpqrcv_PQexec](libpqrcv_PQexec.md): Internal wrapper for PQexec with error handling
  - [PQresultStatus](../P/PQresultStatus.md): libpq function to check result status
  - [PQgetvalue](../P/PQgetvalue.md): libpq function to extract field values from query results
  - [PQgetisnull](../P/PQgetisnull.md): libpq function to check for NULL values in results
  - [DatumGetLSN](../D/DatumGetLSN.md)/DirectFunctionCall1Coll: PostgreSQL type conversion functions
  - [pg_lsn_in](../p/pg_lsn_in.md): PostgreSQL function to parse LSN from string format
  - [pstrdup](../p/pstrdup.md): PostgreSQL memory-managed string duplication
  - [pchomp](../p/pchomp.md): PostgreSQL utility to clean error messages
- Called from (representative examples):
  - Referenced by WalReceiverConn structure initialization
  - Used by WAL receiver processes during slot creation operations

## Notes and Other Information
- This is a static function, accessible only within libpqwalreceiver.c
- The function automatically detects server version (15.0+) to use appropriate command syntax
- For logical slots, always uses 'pgoutput' as the output plugin
- Physical slots automatically include RESERVE_WAL to prevent premature WAL cleanup
- Returns allocated memory for snapshot name - caller is responsible for freeing
- Error handling follows PostgreSQL conventions with ereport() for failures
- The LSN returned represents the consistent point from which replication can begin
- Location: src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:1010-1122

## Simplified Source

```c
static char *
libpqrcv_create_slot(WalReceiverConn *conn, const char *slotname, bool temporary,
                     bool two_phase, bool failover, CRSSnapshotAction snapshot_action,
                     XLogRecPtr *lsn)
{
    PGresult *res;
    StringInfoData cmd;
    char *snapshot;
    bool use_new_options_syntax = (PQserverVersion(conn->streamConn) >= 150000);

    initStringInfo(&cmd);

    // Build CREATE_REPLICATION_SLOT command
    appendStringInfo(&cmd, "CREATE_REPLICATION_SLOT \"%s\"", slotname);

    if (temporary)
        appendStringInfoString(&cmd, " TEMPORARY");

    if (conn->logical) {
        // Logical slot with pgoutput plugin
        appendStringInfoString(&cmd, " LOGICAL pgoutput ");
        if (use_new_options_syntax)
            appendStringInfoChar(&cmd, '(');

        // Add optional features
        if (two_phase) {
            appendStringInfoString(&cmd, "TWO_PHASE");
            appendStringInfoString(&cmd, use_new_options_syntax ? ", " : " ");
        }

        if (failover) {
            appendStringInfoString(&cmd, "FAILOVER");
            appendStringInfoString(&cmd, use_new_options_syntax ? ", " : " ");
        }

        // Add snapshot action
        if (use_new_options_syntax) {
            switch (snapshot_action) {
                case CRS_EXPORT_SNAPSHOT:
                    appendStringInfoString(&cmd, "SNAPSHOT 'export'");
                    break;
                case CRS_NOEXPORT_SNAPSHOT:
                    appendStringInfoString(&cmd, "SNAPSHOT 'nothing'");
                    break;
                case CRS_USE_SNAPSHOT:
                    appendStringInfoString(&cmd, "SNAPSHOT 'use'");
                    break;
            }
            appendStringInfoChar(&cmd, ')');
        } else {
            switch (snapshot_action) {
                case CRS_EXPORT_SNAPSHOT:
                    appendStringInfoString(&cmd, "EXPORT_SNAPSHOT");
                    break;
                case CRS_NOEXPORT_SNAPSHOT:
                    appendStringInfoString(&cmd, "NOEXPORT_SNAPSHOT");
                    break;
                case CRS_USE_SNAPSHOT:
                    appendStringInfoString(&cmd, "USE_SNAPSHOT");
                    break;
            }
        }
    } else {
        // Physical slot
        if (use_new_options_syntax)
            appendStringInfoString(&cmd, " PHYSICAL (RESERVE_WAL)");
        else
            appendStringInfoString(&cmd, " PHYSICAL RESERVE_WAL");
    }

    // Execute slot creation
    res = libpqrcv_PQexec(conn->streamConn, cmd.data);
    pfree(cmd.data);

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQclear(res);
        ereport(ERROR, (errmsg("could not create replication slot \"%s\": %s",
                               slotname, pchomp(PQerrorMessage(conn->streamConn)))));
    }

    // Extract LSN and snapshot from result
    if (lsn)
        *lsn = DatumGetLSN(DirectFunctionCall1Coll(pg_lsn_in, InvalidOid,
                                                   CStringGetDatum(PQgetvalue(res, 0, 1))));

    if (!PQgetisnull(res, 0, 2))
        snapshot = pstrdup(PQgetvalue(res, 0, 2));
    else
        snapshot = NULL;

    PQclear(res);
    return snapshot;
}
```