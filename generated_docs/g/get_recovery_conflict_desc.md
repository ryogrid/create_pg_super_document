# get_recovery_conflict_desc

## Location
[src/backend/storage/ipc/standby.c:1484-1516](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/standby.c#L1484-L1516)

## Overview
Returns a human-readable description string for different types of PostgreSQL recovery conflicts used in standby servers during hot standby operations.

## Definition
static const char *get_recovery_conflict_desc(ProcSignalReason reason)

## Detailed Description
This static function serves as a utility for converting  enumeration values into localized, human-readable description strings for recovery conflict scenarios. It is primarily used for logging purposes when recovery conflicts occur during PostgreSQL hot standby operations. The function uses a switch statement to map specific recovery conflict signal reasons to their corresponding descriptive messages, with all messages being marked for internationalization using the  macro.

The function handles seven distinct types of recovery conflicts that can occur when a standby server needs to resolve conflicts with user queries during the replay of WAL records from the primary server.

## Parameters / Member Variables
- : A  enumeration value representing the specific type of recovery conflict that occurred

## Dependencies
- Functions called/Symbols referenced:
  -  (enum parameter)
  - 
  - 
  - 
  - 
  - 
  - 
  - 
- Called from:
  -  (standby.c:330, 339)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the standby.c file
- All returned strings are wrapped with the  macro for internationalization support
- The function provides descriptions for the following conflict types:
  - **Buffer pin conflicts**: When a query holds a buffer pin that conflicts with WAL replay
  - **Lock conflicts**: When a query holds a lock that prevents WAL replay from proceeding
  - **Tablespace conflicts**: When a tablespace is being dropped but queries are still accessing it
  - **Snapshot conflicts**: When a query's snapshot conflicts with cleanup operations during recovery
  - **Replication slot conflicts**: When logical replication slots conflict with recovery operations
  - **Buffer deadlock conflicts**: When there's a deadlock involving buffer access during recovery
  - **Database conflicts**: When a database is being dropped but connections still exist
- Returns "unknown reason" as a fallback for unrecognized signal reasons
- Used exclusively in recovery conflict logging to provide clear, user-friendly error messages in PostgreSQL logs

## Simplified Source

```c
static const char *get_recovery_conflict_desc(ProcSignalReason reason) {
    const char *reasonDesc = _("unknown reason");

    switch (reason) {
        case PROCSIG_RECOVERY_CONFLICT_BUFFERPIN:
            reasonDesc = _("recovery conflict on buffer pin");
            break;
        case PROCSIG_RECOVERY_CONFLICT_LOCK:
            reasonDesc = _("recovery conflict on lock");
            break;
        case PROCSIG_RECOVERY_CONFLICT_TABLESPACE:
            reasonDesc = _("recovery conflict on tablespace");
            break;
        case PROCSIG_RECOVERY_CONFLICT_SNAPSHOT:
            reasonDesc = _("recovery conflict on snapshot");
            break;
        case PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT:
            reasonDesc = _("recovery conflict on replication slot");
            break;
        case PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK:
            reasonDesc = _("recovery conflict on buffer deadlock");
            break;
        case PROCSIG_RECOVERY_CONFLICT_DATABASE:
            reasonDesc = _("recovery conflict on database");
            break;
        default:
            // reasonDesc remains "unknown reason"
            break;
    }

    return reasonDesc;
}
```