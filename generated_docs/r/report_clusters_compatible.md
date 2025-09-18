# report_clusters_compatible

## Location
src/bin/pg_upgrade/check.c: 722 - 740

## Overview
Reports that clusters are compatible for upgrade and handles appropriate action based on whether this is a check-only run or an actual upgrade.

## Definition


## Detailed Description
This function serves as a checkpoint in the pg_upgrade process, indicating that the old and new PostgreSQL clusters have been verified as compatible for upgrade. The function's behavior depends on the operation mode:

1. **Check mode** ( is true): Reports compatibility, stops the new cluster, cleans up output directories, and exits with status 0
2. **Upgrade mode**: Issues a warning message that if pg_upgrade fails after this point, the new cluster must be re-initialized before continuing

The function acts as a critical decision point where pg_upgrade either completes successfully (in check mode) or proceeds with the actual upgrade process.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  -  (with PG_REPORT level)
  - 
  - 
  - 
  -  (global variable)
  -  (log level constant)
- Called from (representative examples):
  -  (in src/bin/pg_upgrade/pg_upgrade.c:144)

## Notes and Other Information
- This function represents a "point of no return" in the upgrade process - after this point, failure requires re-initializing the new cluster
- In check mode, this function terminates the program successfully, indicating that the upgrade would be feasible
- The warning message emphasizes the critical nature of this checkpoint in the upgrade workflow