# check_cluster_compatibility

## Location
[src/bin/pg_upgrade/check.c:839-852](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/check.c#L839-L852)

## Overview
Retrieves and validates pg_control data from both clusters and ensures port configuration is appropriate for live server checks.

## Definition


## Detailed Description
This function performs low-level compatibility checks between the old and new PostgreSQL clusters by examining their control data structures. The function executes several key validation steps:

1. **Control Data Retrieval**: Gathers pg_control information from both the old cluster (respecting the live_check parameter) and the new cluster (always offline).

2. **Control Data Validation**: Compares critical pg_control fields between clusters to ensure they are compatible for upgrade, including system identifiers, data checksums, and other fundamental database parameters.

3. **Live Server Port Validation**: When performing live checks (checking a running old cluster), ensures that the old and new clusters use different port numbers to prevent conflicts.

The function is essential for validating that the two clusters can be safely upgraded, focusing on low-level database internals that must be compatible.

## Parameters / Member Variables
- : Boolean indicating whether the old cluster is running and should be checked as a live server. When true, enables additional validations specific to live server scenarios and affects how control data is retrieved from the old cluster.

## Dependencies
- Functions called/Symbols referenced:
  -  (retrieves pg_control information from a cluster)
  -  (validates compatibility between control data structures)
  -  (terminates with error message)
  -  (global old cluster information structure)
  -  (global new cluster information structure)
  -  (control data from old cluster)
  -  (control data from new cluster)  
  -  (old cluster port number)
  -  (new cluster port number)
- Called from (representative examples):
  -  (in src/bin/pg_upgrade/pg_upgrade.c:135)

## Notes and Other Information
- This function focuses on PostgreSQL's internal control data compatibility rather than high-level version checks
- The live_check parameter affects how the old cluster's control data is retrieved - live servers require different handling than stopped clusters
- [Port](../P/Port.md) conflict checking is critical for live upgrades to prevent the upgrade process from interfering with the running old cluster
- Control data validation includes checks for system identifiers, page checksums, WAL block sizes, and other fundamental database parameters
- This function complements version checking by examining the actual database internals rather than just version numbers