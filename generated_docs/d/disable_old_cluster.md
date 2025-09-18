# disable_old_cluster

## Location
src/bin/pg_upgrade/controldata.c: 711 - 732

## Overview
Prevents accidental startup of the old PostgreSQL cluster after a successful upgrade by renaming its control file.

## Definition


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
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - prep_status (status reporting initialization)
  - pg_mv_file (safe file moving utility)  
  - check_ok (status completion reporting)
  - pg_log (logging with PG_REPORT level)
  - old_cluster (global cluster information structure)
- Called from (representative examples):
  - main (src/bin/pg_upgrade/pg_upgrade.c:181)

## Notes and Other Information
- Critical safety function that prevents data corruption in link-mode upgrades
- The old cluster can be recovered by manually removing the  suffix from the control file
- Particularly important for link-mode upgrades where data files are shared between clusters
- The function provides clear user guidance about the implications and recovery procedures
- Part of the final cleanup phase in the pg_upgrade process