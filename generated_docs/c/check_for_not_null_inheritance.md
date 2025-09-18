# check_for_not_null_inheritance

## Location
src/bin/pg_upgrade/check.c: 1596 - 1672

## Overview
Validates that child tables do not lack NOT NULL constraints that are present in their parent tables during PostgreSQL cluster upgrades.

## Definition
```c
static void check_for_not_null_inheritance(ClusterInfo *cluster)
```

## Detailed Description
This function checks for inheritance inconsistencies where child tables have columns that lack NOT NULL constraints while their corresponding parent table columns have them. Such inconsistencies were possible in PostgreSQL versions prior to version 18 but can no longer occur. The function prevents upgrade failures by identifying these problematic cases and requiring manual fixes before the upgrade can proceed.

The function performs the following operations:
- Iterates through all databases in the old cluster
- Executes a complex SQL query to identify inheritance constraint mismatches
- Writes problematic table.column combinations to a report file
- Terminates the upgrade process with detailed instructions if issues are found

## Parameters / Member Variables
- `cluster`: Pointer to ClusterInfo structure containing information about the PostgreSQL cluster being checked

## Dependencies
- Functions called/Symbols referenced:
  - prep_status
  - connectToServer
  - executeQueryOrDie
  - fopen_priv
  - PQfinish
  - pg_log
  - check_ok
- Called from:
  - check_and_dump_old_cluster

## Notes and Other Information
- This is a static function specific to pg_upgrade functionality
- Creates an output file "not_null_inconsistent_columns.txt" in the log base directory when issues are found
- Uses a complex SQL query joining pg_inherits, pg_attribute, pg_class, and pg_namespace system catalogs
- The function terminates the entire upgrade process if any inconsistencies are detected
- Provides specific ALTER TABLE commands to fix identified issues
- Part of PostgreSQL's cluster upgrade safety checks introduced to prevent upgrade failures