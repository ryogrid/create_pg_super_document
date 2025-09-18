# get_control_data

## Location
src/bin/pg_upgrade/controldata.c: 36 - 653

## Overview
Extracts pg_control information from PostgreSQL clusters in a version-independent manner by invoking pg_controldata or pg_resetwal and parsing their output.

## Definition


## Detailed Description
The  function is a core component of pg_upgrade that extracts critical control data from PostgreSQL clusters. It handles version differences by using different utilities:

- For live checks or when examining the new cluster: Uses  to read control information from a running or shutdown server
- For offline checks on the old cluster: Uses  (or  for versions ≤ 9.6) to simulate what the control data would be after a reset

The function sets up a controlled environment by manipulating locale variables to ensure English output, then parses the utility output line-by-line to extract essential parameters like transaction IDs, checkpoint information, database configuration parameters, and WAL settings.

Key validation includes:
- Verifying cluster shutdown state (must be cleanly shut down, not in recovery)
- Ensuring all mandatory control data fields are present
- Handling version-specific output format differences
- Constructing WAL filenames for older versions (≤ 9.2) from separate log ID and segment components

## Parameters / Member Variables
- : ClusterInfo structure to populate with extracted control data
- : Boolean indicating whether this is a live server check (true) or offline analysis (false)

## Dependencies
- Functions called/Symbols referenced:
  - popen/pclose (system process execution)
  - pg_strip_crlf (string processing)
  - [str2uint](../s/str2uint.md) (string to integer conversion)
  - setenv/unsetenv (environment manipulation)
  - [pg_log](../p/pg_log.md) (logging)
  - [pg_fatal](../p/pg_fatal.md) (error handling)
  - strlcpy (safe string copying)
  - [pg_free](../p/pg_free.md) (memory management)
- Called from (representative examples):
  - [check_cluster_compatibility](../c/check_cluster_compatibility.md) (src/bin/pg_upgrade/check.c:842-843)

## Notes and Other Information
- Temporarily modifies locale environment variables to force English output for reliable parsing
- Handles multiple PostgreSQL version differences in control data format and utility names
- Critical for upgrade compatibility checking as it provides the foundation data for comparing old and new clusters
- The function includes extensive error checking and detailed reporting of missing control information
- WAL filename construction differs between PostgreSQL versions, requiring version-specific logic