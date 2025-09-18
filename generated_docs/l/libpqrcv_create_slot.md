# libpqrcv_create_slot

## Location
src/backend/replication/libpqwalreceiver/libpqwalreceiver.c: 1010 - 1122

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
  - initStringInfo/appendStringInfo: PostgreSQL string manipulation functions
  - [libpqrcv_PQexec](libpqrcv_PQexec.md): Internal wrapper for PQexec with error handling
  - [PQresultStatus](../P/PQresultStatus.md): libpq function to check result status
  - [PQgetvalue](../P/PQgetvalue.md): libpq function to extract field values from query results
  - [PQgetisnull](../P/PQgetisnull.md): libpq function to check for NULL values in results
  - DatumGetLSN/DirectFunctionCall1Coll: PostgreSQL type conversion functions
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