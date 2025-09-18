# check_for_user_defined_encoding_conversions

## Location
src/bin/pg_upgrade/check.c: 1728 - 1811

## Overview
Validates that the old PostgreSQL cluster does not contain any user-defined encoding conversions, which are incompatible with PostgreSQL version 14 and later due to function parameter changes.

## Definition
```c
static void check_for_user_defined_encoding_conversions(ClusterInfo *cluster)
```

## Detailed Description
This function checks for user-defined encoding conversions in the old PostgreSQL cluster that would prevent successful upgrade to version 14 or later. In PostgreSQL version 14, the conversion function parameters changed, making existing user-defined encoding conversions incompatible. The function identifies these conversions and requires their removal before upgrade can proceed.

The function performs the following operations:
- Iterates through all databases in the cluster
- Queries pg_catalog.pg_conversion for user-defined conversions (OID >= 16384)
- Joins with pg_catalog.pg_namespace to get namespace information
- Writes problematic conversions to a report file with OID, namespace, and name
- Terminates the upgrade process if any user-defined conversions are found

## Parameters / Member Variables
- `cluster`: Pointer to ClusterInfo structure containing information about the PostgreSQL cluster being checked

## Dependencies
- Functions called/Symbols referenced:
  - [prep_status](../p/prep_status.md)
  - [connectToServer](connectToServer.md)
  - [executeQueryOrDie](../e/executeQueryOrDie.md)
  - fopen_priv
  - [PQclear](../P/PQclear.md)
  - [PQfinish](../P/PQfinish.md)
  - [pg_log](../p/pg_log.md)
  - [check_ok](check_ok.md)
- Called from:
  - [check_and_dump_old_cluster](check_and_dump_old_cluster.md)

## Notes and Other Information
- This is a static function specific to pg_upgrade functionality
- Creates an output file "encoding_conversions.txt" in the log base directory when problematic conversions are found
- Uses FirstNormalObjectId (16384) as a hardcoded cutoff to distinguish user-defined from system conversions
- The hardcoded value ensures consistent behavior regardless of future changes to the C #define
- Specifically addresses compatibility issues introduced in PostgreSQL version 14
- Provides clear guidance that conversions must be removed before upgrade can proceed
- Part of PostgreSQL's upgrade validation to prevent incompatibility with conversion function changes