# get_major_server_version

## Location
src/bin/pg_upgrade/server.c: 159 - 190

## Overview
Retrieves the major PostgreSQL server version number by reading and parsing the PG_VERSION file from the cluster's data directory.

## Definition
```c
uint32 get_major_server_version(ClusterInfo *cluster)
```

## Detailed Description
This function determines the major PostgreSQL version by reading the PG_VERSION file located in the cluster's data directory. It handles both old-style version numbering (e.g., 9.6.1) and new-style version numbering (e.g., 10.1, 11.0) that was introduced in PostgreSQL 10. The function parses the version string and converts it to a standardized integer format for easier version comparisons. For versions prior to 10, it uses the format major*10000 + minor*100, while for version 10 and later, it uses major*10000.

## Parameters / Member Variables
- `cluster`: Pointer to ClusterInfo structure containing the pgdata directory path

## Dependencies
- Functions called/Symbols referenced:
  - snprintf
  - fopen
  - pg_fatal
  - fscanf
  - sscanf
  - fclose
- Called from (representative examples):
  - check_data_dir
  - fopen_priv

## Notes and Other Information
- Reads PG_VERSION file directly from the PostgreSQL data directory
- Handles version format changes introduced in PostgreSQL 10.x
- Stores the version string in cluster->major_version_str for later reference
- Returns standardized integer representation enabling easy version comparisons
- Uses pg_fatal for error handling - any failure results in program termination
- Essential for pg_upgrade to determine compatibility and required upgrade procedures
- Version format: Pre-10 uses X.Y format, 10+ uses single major number format