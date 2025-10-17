# setup_recovery

## Location
[src/bin/pg_basebackup/pg_createsubscriber.c:1183-1251](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_createsubscriber.c#L1183-L1251)

## Overview
Configures PostgreSQL recovery parameters for the standby-to-subscriber conversion process, setting up the recovery configuration file with appropriate LSN targets and recovery behavior.

## Definition
```c
static void setup_recovery(const struct LogicalRepInfo *dbinfo, const char *datadir, const char *lsn)
```

## Detailed Description
This function prepares the recovery configuration for a PostgreSQL standby server that is being converted to a logical subscriber. It generates a recovery.conf file with specific parameters to control the recovery process, including setting the recovery target to a specific LSN, configuring recovery to promote the server automatically, and preventing the reapplication of transactions that will be handled by logical replication. The function handles both dry-run and actual execution modes, and ensures recovery stops at the exact point where logical replication should begin.

## Parameters / Member Variables
- `dbinfo`: Array of LogicalRepInfo structures containing database and connection information (uses the first element for publisher connection info)
- `datadir`: Path to the PostgreSQL data directory where recovery.conf will be written
- `lsn`: Log Sequence Number string specifying the exact recovery target point

## Dependencies
- Functions called/Symbols referenced:
  - [connect_database](../c/connect_database.md) (connects using publisher connection info)
  - [GenerateRecoveryConfig](../G/GenerateRecoveryConfig.md) (generates base recovery configuration)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md) (builds recovery configuration content)
  - [WriteRecoveryConfig](../W/WriteRecoveryConfig.md) (writes the configuration to disk)
  - [disconnect_database](../d/disconnect_database.md) (closes database connection)
  - pg_log_debug (logs recovery parameters for debugging)
- Called from:
  - [main](../m/main.md) (primary entry point of pg_createsubscriber utility)

## Notes and Other Information
- This is a static function, only accessible within pg_createsubscriber.c
- Sets recovery_target_inclusive = false to prevent duplicate transaction application
- Configures recovery_target_action = promote for automatic promotion after recovery
- Clears other recovery target settings to avoid conflicts
- Supports dry-run mode with invalid LSN for testing
- Critical for ensuring smooth transition from physical to logical replication
- Uses publisher connection info despite writing subscriber recovery parameters
- Part of the standby-to-subscriber conversion workflow

## Simplified Source

```c
static void
setup_recovery(const struct LogicalRepInfo *dbinfo, const char *datadir, const char *lsn)
{
    PGconn *conn;
    PQExpBuffer recoveryconfcontents;

    // Connect using publisher connection info to generate recovery config
    conn = connect_database(dbinfo[0].pubconninfo, true);

    // Generate base recovery configuration
    recoveryconfcontents = GenerateRecoveryConfig(conn, NULL, NULL);

    // Add recovery parameters to control behavior
    appendPQExpBuffer(recoveryconfcontents, "recovery_target = ''\n");
    appendPQExpBuffer(recoveryconfcontents, "recovery_target_timeline = 'latest'\n");

    // Set recovery_target_inclusive = false to prevent duplicate transactions
    // after logical replication starts
    appendPQExpBuffer(recoveryconfcontents, "recovery_target_inclusive = false\n");
    appendPQExpBuffer(recoveryconfcontents, "recovery_target_action = promote\n");

    // Clear other recovery target settings to avoid conflicts
    appendPQExpBuffer(recoveryconfcontents, "recovery_target_name = ''\n");
    appendPQExpBuffer(recoveryconfcontents, "recovery_target_time = ''\n");
    appendPQExpBuffer(recoveryconfcontents, "recovery_target_xid = ''\n");

    // Set recovery target LSN based on mode
    if (dry_run) {
        appendPQExpBuffer(recoveryconfcontents, "# dry run mode");
        appendPQExpBuffer(recoveryconfcontents, "recovery_target_lsn = '%X/%X'\n",
                          LSN_FORMAT_ARGS((XLogRecPtr) InvalidXLogRecPtr));
    } else {
        appendPQExpBuffer(recoveryconfcontents, "recovery_target_lsn = '%s'\n", lsn);
        WriteRecoveryConfig(conn, datadir, recoveryconfcontents);
    }

    disconnect_database(conn, false);

    pg_log_debug("recovery parameters:\n%s", recoveryconfcontents->data);
}
```