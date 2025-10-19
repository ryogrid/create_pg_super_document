# sanityChecks

## Location
[src/bin/pg_rewind/pg_rewind.c:733-790](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/pg_rewind.c#L733-L790)

## Overview
The  function validates that the source and target PostgreSQL clusters are compatible and in appropriate states before proceeding with a rewind operation.

## Definition

```c
static void
sanityChecks(void)
```
## Detailed Description
The  function performs critical pre-flight validation checks before pg_rewind begins its rewind operation. These checks ensure that the operation will be safe and successful by verifying compatibility and operational prerequisites between the source and target clusters.

The function performs the following validation checks:

1. **System identifier matching**: Ensures both clusters originated from the same initial database cluster by comparing system_identifier values
2. **Version compatibility**: Validates that both clusters are using compatible PostgreSQL versions by checking:
   - pg_control_version (control file format version)
   - catalog_version_no (system catalog version)
3. **Data integrity prerequisites**: Ensures the target cluster has either:
   - Data checksums enabled, OR
   - WAL hint bits logging enabled (wal_log_hints = on)
   This prevents data corruption from unlogged hint bit changes
4. **Target cluster state**: Verifies the target server is properly shut down (either DB_SHUTDOWNED or DB_SHUTDOWNED_IN_RECOVERY)
5. **Source cluster state** (when using local directory): If the source is a local data directory, ensures it is also properly shut down

Any validation failure results in a fatal error that terminates the pg_rewind operation.

## Parameters / Member Variables
None - the function operates on global control file variables:
- : Target cluster's control file data
- : Source cluster's control file data
- : Boolean indicating if source is a local directory

## Dependencies
- Functions called/Symbols referenced:
  -  (error reporting and termination)
  -  (expected control file version)
  -  (expected catalog version)
  -  (data checksum version constant)
  -  (clean shutdown state)
  -  (clean shutdown during recovery state)

- Called from (representative examples):
  -  at src/bin/pg_rewind/pg_rewind.c:350

## Notes and Other Information
- This is a static function only accessible within pg_rewind.c
- Contains a TODO comment about checking for backup_label files in either cluster
- The version checks ensure compatibility with the current version of pg_rewind
- The hint bits/checksums requirement is crucial for preventing data corruption during rewind
- Target shutdown requirement prevents concurrent access during rewind operation
- Source shutdown requirement (for local directories) is described as precautionary rather than strictly necessary
- All validation failures are fatal and immediately terminate the program
- The function performs "fail-fast" validation to catch problems before any modifications begin
- Located at src/bin/pg_rewind/pg_rewind.c:733-790

## Simplified Source

```c
static void sanityChecks(void)
{
    // Check that both clusters originated from same initial database
    if (ControlFile_target.system_identifier != ControlFile_source.system_identifier)
        pg_fatal("source and target clusters are from different systems");

    // Verify version compatibility
    if (ControlFile_target.pg_control_version != PG_CONTROL_VERSION ||
        ControlFile_source.pg_control_version != PG_CONTROL_VERSION ||
        ControlFile_target.catalog_version_no != CATALOG_VERSION_NO ||
        ControlFile_source.catalog_version_no != CATALOG_VERSION_NO) {
        pg_fatal("clusters are not compatible with this version of pg_rewind");
    }

    // Ensure target has data integrity protection
    if (ControlFile_target.data_checksum_version != PG_DATA_CHECKSUM_VERSION &&
        !ControlFile_target.wal_log_hints) {
        pg_fatal("target server needs to use either data checksums or \"wal_log_hints = on\"");
    }

    // Verify target server is properly shut down
    if (ControlFile_target.state != DB_SHUTDOWNED &&
        ControlFile_target.state != DB_SHUTDOWNED_IN_RECOVERY)
        pg_fatal("target server must be shut down cleanly");

    // For local directory sources, also verify source shutdown
    if (datadir_source &&
        ControlFile_source.state != DB_SHUTDOWNED &&
        ControlFile_source.state != DB_SHUTDOWNED_IN_RECOVERY)
        pg_fatal("source data directory must be shut down cleanly");
}
```