# cleanup_objects_atexit

## Location
[src/bin/pg_basebackup/pg_createsubscriber.c:157-216](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_createsubscriber.c#L157-L216)

## Overview
Cleanup function that removes PostgreSQL objects (publications and replication slots) that were created by pg_createsubscriber when an error occurs during the subscription setup process.

## Definition

```c
static void
cleanup_objects_atexit(void)
```
## Detailed Description
This function serves as an error handler registered with atexit() to perform cleanup operations when pg_createsubscriber fails. It attempts to remove publications and replication slots that were created on the primary server during the subscription setup process. The function operates differently depending on whether the target server has been promoted or not:

- If recovery has ended (server promoted), it warns the user that the physical replica cannot be reused
- For each database, it attempts to connect and drop any publications or replication slots that were created
- If connection fails, it logs warnings about objects left behind on the primary
- If the standby server is running, it stops it

The function only executes if the global  flag is false, indicating an error occurred.

## Parameters / Member Variables
This function takes no parameters but operates on several global variables:
- : Global flag indicating if the operation completed successfully
- : Flag indicating if recovery has ended (server promoted)
- : Number of databases being processed
- : Array of database information structures containing publication and replication slot details
- : Flag indicating if the standby server is running
- : Directory path for the subscriber

## Dependencies
- Functions called/Symbols referenced:
  - pg_log_warning
  - pg_log_warning_hint
  - [connect_database](connect_database.md)
  - [drop_publication](../d/drop_publication.md)
  - [drop_replication_slot](../d/drop_replication_slot.md)
  - [disconnect_database](../d/disconnect_database.md)
  - [stop_standby_server](../s/stop_standby_server.md)
- Called from (representative examples):
  - [main](../m/main.md) (registered as atexit handler)

## Notes and Other Information
- This is a static function specific to pg_createsubscriber utility
- Designed as a best-effort cleanup - if connections fail, it logs warnings instead of failing
- Critical for preventing resource leaks on the primary server when subscription setup fails
- Does not attempt cleanup on the target server after promotion, as the replica would need to be recreated anyway
- Uses conditional cleanup based on flags tracking what objects were actually created during the process

## Simplified Source

```c
static void cleanup_objects_atexit(void) {
    // Only run cleanup if operation failed
    if (success) {
        return;
    }

    // Warn user if recovery ended - replica needs recreation
    if (recovery_ended) {
        pg_log_warning("failed after the end of recovery");
        pg_log_warning_hint("The target server cannot be used as a physical replica anymore. "
                           "You must recreate the physical replica before continuing.");
    }

    // Clean up publications and replication slots for each database
    for (int i = 0; i < num_dbs; i++) {
        if (dbinfo[i].made_publication || dbinfo[i].made_replslot) {
            PGconn *conn = connect_database(dbinfo[i].pubconninfo, false);

            if (conn != NULL) {
                // Successfully connected - clean up objects
                if (dbinfo[i].made_publication) {
                    drop_publication(conn, &dbinfo[i]);
                }
                if (dbinfo[i].made_replslot) {
                    drop_replication_slot(conn, &dbinfo[i], dbinfo[i].replslotname);
                }
                disconnect_database(conn, false);
            } else {
                // Connection failed - warn about leftover objects
                if (dbinfo[i].made_publication) {
                    pg_log_warning("publication \"%s\" left behind in database \"%s\"",
                                 dbinfo[i].pubname, dbinfo[i].dbname);
                    pg_log_warning_hint("Drop this publication before trying again.");
                }
                if (dbinfo[i].made_replslot) {
                    pg_log_warning("replication slot \"%s\" left behind in database \"%s\"",
                                 dbinfo[i].replslotname, dbinfo[i].dbname);
                    pg_log_warning_hint("Drop this replication slot to avoid WAL retention.");
                }
            }
        }
    }

    // Stop standby server if running
    if (standby_running) {
        stop_standby_server(subscriber_dir);
    }
}
```