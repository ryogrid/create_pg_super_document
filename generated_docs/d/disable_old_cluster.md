# disable_old_cluster

## Location
[src/bin/pg_upgrade/controldata.c:711-732](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/controldata.c#L711-L732)

## Overview
Prevents accidental startup of the old PostgreSQL cluster after a successful upgrade by renaming its control file.

## Definition

```c
void
disable_old_cluster(void)
```
## Detailed Description
The  function is a safety mechanism executed at the completion of a PostgreSQL upgrade process. It permanently disables the old cluster by renaming the critical  file, which is essential for PostgreSQL server startup.

The function performs the following operations:
1. Constructs file paths for the old cluster's  file
2. Renames the control file from  to 
3. Reports the operation status to the user
4. Provides instructions for potential recovery if needed

This safety measure is particularly important when using "link" mode during upgrades, where the old and new clusters share data files through hard links. Starting the old cluster after the new cluster has been started could lead to serious data corruption, as both would attempt to modify the same underlying files.

The function ensures that administrators cannot accidentally start the old cluster, while still providing a recovery path by simply removing the  suffix if needed.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [prep_status](../p/prep_status.md) (status reporting initialization)
  - pg_mv_file (safe file moving utility)  
  - [check_ok](../c/check_ok.md) (status completion reporting)
  - [pg_log](../p/pg_log.md) (logging with PG_REPORT level)
  - old_cluster (global cluster information structure)
- Called from (representative examples):
  - [main](../m/main.md) (src/bin/pg_upgrade/pg_upgrade.c:181)

## Notes and Other Information
- Critical safety function that prevents data corruption in link-mode upgrades
- The old cluster can be recovered by manually removing the  suffix from the control file
- Particularly important for link-mode upgrades where data files are shared between clusters
- The function provides clear user guidance about the implications and recovery procedures
- Part of the final cleanup phase in the pg_upgrade process

## Simplified Source

```c
void disable_old_cluster(void) {
    char old_path[MAXPGPATH], new_path[MAXPGPATH];

    // Report status to user
    prep_status("Adding \".old\" suffix to old global/pg_control");

    // Build paths for old and new control file locations
    snprintf(old_path, sizeof(old_path), "%s/global/pg_control", old_cluster.pgdata);
    snprintf(new_path, sizeof(new_path), "%s/global/pg_control.old", old_cluster.pgdata);

    // Rename control file to disable old cluster
    if (pg_mv_file(old_path, new_path) != 0)
        pg_fatal("could not rename file \"%s\" to \"%s\": %m", old_path, new_path);

    check_ok();

    // Inform user about the change and recovery procedure
    pg_log(PG_REPORT, "If you want to start the old cluster, you will need to remove "
           "the \".old\" suffix from %s/global/pg_control.old. "
           "Because \"link\" mode was used, the old cluster cannot be safely "
           "started once the new cluster has been started.", old_cluster.pgdata);
}
```