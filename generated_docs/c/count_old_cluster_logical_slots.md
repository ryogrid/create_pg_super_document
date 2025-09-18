# count_old_cluster_logical_slots

## Location
src/bin/pg_upgrade/info.c: 732 - 747

## Overview
The count_old_cluster_logical_slots function returns the total number of logical replication slots across all databases in the old PostgreSQL cluster.

## Definition


## Detailed Description
This utility function provides a simple count of all logical replication slots present in the old cluster by iterating through all databases and summing their slot counts. It serves as a convenience function for other parts of pg_upgrade that need to determine whether logical slots exist before performing slot-related operations. The function leverages the slot information previously collected by get_old_cluster_logical_slot_infos() and stored in each database's slot_arr structure. For PostgreSQL 16 and earlier clusters, this function always returns 0 since logical slot information is not collected for those versions due to reliability concerns with slot state persistence.

## Parameters / Member Variables
- None (void parameter list)

## Dependencies
- Functions called/Symbols referenced:
  - None (only accesses global old_cluster structure)
- Called from (representative examples):
  - check_new_cluster_logical_replication_slots
  - get_loadable_libraries
  - main
  - fopen_priv

## Notes and Other Information
- Returns 0 for PostgreSQL 16 and earlier clusters as slot information is not collected for these versions
- Relies on slot information previously populated by get_old_cluster_logical_slot_infos()
- Iterates through old_cluster.dbarr.dbs array to access each database's slot count
- Used primarily for conditional logic to determine if slot-related upgrade steps are necessary
- Function has external linkage (not static), making it available to other compilation units
- Provides a simple interface to avoid code duplication when checking for presence of logical slots