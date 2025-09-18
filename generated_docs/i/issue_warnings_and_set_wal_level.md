# issue_warnings_and_set_wal_level

## Location
[src/bin/pg_upgrade/check.c:741-761](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/check.c#L741-L761)

## Overview
Performs final setup tasks after upgrade compatibility is confirmed, including setting WAL level, handling legacy hash indexes, and reporting extension updates.

## Definition


## Detailed Description
This function executes several critical post-compatibility tasks during the pg_upgrade process:

1. **WAL Level Management**: Starts the new cluster to ensure proper WAL level settings. This is necessary because pg_resetwal sets wal_level to 'minimum', but standby servers upgraded using rsync instructions need the final WAL record to show wal_level as 'replica'.

2. **Legacy Hash Index Handling**: For upgrades from PostgreSQL versions 9.6 and earlier (major version <= 906), invalidates hash indexes since the hash index format changed in version 10.0.

3. **Extension Updates**: Reports any extension updates that may be needed in the new cluster.

4. **Cleanup**: Stops the postmaster after completing these tasks.

The function ensures that the new cluster is properly configured and that any version-specific compatibility issues are addressed.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  -  (starts new cluster with WAL writing enabled)
  -  (macro to extract major version number)  
  -  (handles hash index compatibility for old versions)
  -  (reports extension update requirements)
  -  (stops the cluster)
  -  (global cluster information structure)
  -  (source cluster version information)
- Called from (representative examples):
  -  (in src/bin/pg_upgrade/pg_upgrade.c:227)

## Notes and Other Information
- The function is called after compatibility checks pass but before the actual upgrade process begins
- WAL level handling is critical for standby server upgrades using rsync methodology
- Hash index invalidation is a one-time migration task for very old PostgreSQL versions (< 10.0)  
- This function bridges the gap between compatibility verification and actual data migration
- The temporary start/stop of the new cluster is intentional and necessary for proper WAL record generation