# copy_xact_xlog_xid

## Location
[src/bin/pg_upgrade/pg_upgrade.c:702-826](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/pg_upgrade.c#L702-L826)

## Overview
Copies transaction commit logs and multixact data from the old cluster to the new cluster, then resets transaction IDs, multixact IDs, and WAL archives to maintain transaction continuity during pg_upgrade.

## Definition

```c
static void
copy_xact_xlog_xid(void)
```
## Detailed Description
This function is responsible for preserving transaction state and continuity between the old and new PostgreSQL clusters during an upgrade. It performs several critical operations:

1. **Transaction Log Copy**: Copies commit logs from the old cluster, handling the naming change from pg_clog (pre-v10) to pg_xact (v10+) based on the cluster versions.

2. **Transaction ID Management**: Uses pg_resetwal to set:
   - Oldest XID in the new cluster
   - Next transaction ID and epoch
   - Commit timestamp limits

3. **Multixact Handling**: Conditionally handles multixact data based on format compatibility:
   - **Compatible versions**: Copies pg_multixact/offsets and pg_multixact/members directories and preserves multixact counters
   - **Incompatible versions**: Removes existing multixact offsets and sets appropriate multixact boundaries to prevent reading obsolete data

4. **WAL Archive Reset**: Resets WAL archives in the new cluster using timeline 1 and the appropriate log file sequence from the old cluster.

The function includes extensive error checking and user status updates throughout the process.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - GET_MAJOR_VERSION: Determines PostgreSQL major version for compatibility checks
  - [copy_subdir_files](copy_subdir_files.md): Copies entire subdirectories between clusters
  - [prep_status](../p/prep_status.md): Displays operation status to user
  - [exec_prog](../e/exec_prog.md): Executes pg_resetwal commands with various options
  - [check_ok](check_ok.md): Verifies each operation completed successfully
  - [remove_new_subdir](../r/remove_new_subdir.md): Removes incompatible multixact offset files
- Constants used:
  - UTILITY_LOG_FILE: Log file for utility operations
  - MULTIXACT_FORMATCHANGE_CAT_VER: Version constant for multixact format compatibility
- Data structures used:
  - old_cluster.controldata: Contains transaction and multixact state from old cluster
  - new_cluster: Contains paths and connection info for new cluster
- Called from:
  - [main](../m/main.md): Part of the main pg_upgrade workflow after schema restoration

## Notes and Other Information
- The function handles version-specific directory naming (pg_clog vs pg_xact) automatically
- Multixact handling varies significantly based on format compatibility between cluster versions
- All pg_resetwal operations use the -f flag to force execution without prompts
- The function preserves transaction continuity to prevent data corruption or visibility issues
- WAL archives are reset to timeline 1 with no history file to match a fresh cluster state
- Error handling ensures that any failure in transaction state copying will abort the upgrade
- The commit timestamp limits are set to prevent issues with commit timestamp tracking
- Multixact ID handling prevents the new cluster from attempting to read obsolete multixact data
- The function is critical for maintaining ACID properties across the upgrade process

## Simplified Source

```c
static void copy_xact_xlog_xid(void) {
    // Step 1: Copy transaction commit logs (handle version naming differences)
    copy_subdir_files(GET_MAJOR_VERSION(old_cluster.major_version) <= 906 ?
                      "pg_clog" : "pg_xact",
                      GET_MAJOR_VERSION(new_cluster.major_version) <= 906 ?
                      "pg_clog" : "pg_xact");

    // Step 2: Set oldest XID for new cluster
    prep_status("Setting oldest XID for new cluster");
    exec_prog(UTILITY_LOG_FILE, NULL, true, true,
              "\"%s/pg_resetwal\" -f -u %u \"%s\"",
              new_cluster.bindir, old_cluster.controldata.chkpnt_oldstxid,
              new_cluster.pgdata);
    check_ok();

    // Step 3: Set next transaction ID and epoch
    prep_status("Setting next transaction ID and epoch for new cluster");
    exec_prog(UTILITY_LOG_FILE, NULL, true, true,
              "\"%s/pg_resetwal\" -f -x %u \"%s\"",
              new_cluster.bindir, old_cluster.controldata.chkpnt_nxtxid,
              new_cluster.pgdata);
    exec_prog(UTILITY_LOG_FILE, NULL, true, true,
              "\"%s/pg_resetwal\" -f -e %u \"%s\"",
              new_cluster.bindir, old_cluster.controldata.chkpnt_nxtepoch,
              new_cluster.pgdata);

    // Step 4: Reset commit timestamp limits
    exec_prog(UTILITY_LOG_FILE, NULL, true, true,
              "\"%s/pg_resetwal\" -f -c %u,%u \"%s\"",
              new_cluster.bindir,
              old_cluster.controldata.chkpnt_nxtxid,
              old_cluster.controldata.chkpnt_nxtxid,
              new_cluster.pgdata);
    check_ok();

    // Step 5: Handle multixact data based on version compatibility
    if (old_cluster.controldata.cat_ver >= MULTIXACT_FORMATCHANGE_CAT_VER &&
        new_cluster.controldata.cat_ver >= MULTIXACT_FORMATCHANGE_CAT_VER) {

        // Compatible versions: copy multixact directories
        copy_subdir_files("pg_multixact/offsets", "pg_multixact/offsets");
        copy_subdir_files("pg_multixact/members", "pg_multixact/members");

        prep_status("Setting next multixact ID and offset for new cluster");
        exec_prog(UTILITY_LOG_FILE, NULL, true, true,
                  "\"%s/pg_resetwal\" -O %u -m %u,%u \"%s\"",
                  new_cluster.bindir,
                  old_cluster.controldata.chkpnt_nxtmxoff,
                  old_cluster.controldata.chkpnt_nxtmulti,
                  old_cluster.controldata.chkpnt_oldstMulti,
                  new_cluster.pgdata);
        check_ok();
    } else if (new_cluster.controldata.cat_ver >= MULTIXACT_FORMATCHANGE_CAT_VER) {

        // Incompatible versions: clean up and set boundaries
        remove_new_subdir("pg_multixact/offsets", false);

        prep_status("Setting oldest multixact ID in new cluster");
        exec_prog(UTILITY_LOG_FILE, NULL, true, true,
                  "\"%s/pg_resetwal\" -m %u,%u \"%s\"",
                  new_cluster.bindir,
                  old_cluster.controldata.chkpnt_nxtmulti + 1,
                  old_cluster.controldata.chkpnt_nxtmulti,
                  new_cluster.pgdata);
        check_ok();
    }

    // Step 6: Reset WAL archives
    prep_status("Resetting WAL archives");
    exec_prog(UTILITY_LOG_FILE, NULL, true, true,
              "\"%s/pg_resetwal\" -l 00000001%s \"%s\"",
              new_cluster.bindir,
              old_cluster.controldata.nextxlogfile + 8,  // Skip timeline prefix
              new_cluster.pgdata);
    check_ok();
}
```