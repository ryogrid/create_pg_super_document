# check_for_data_types_usage

## Location
[src/bin/pg_upgrade/check.c:338-537](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/check.c#L338-L537)

## Overview
Detects whether there are any stored columns depending on given problematic data types across all databases in a cluster and generates a report if incompatible types are found.

## Definition

```c
static void
check_for_data_types_usage(ClusterInfo *cluster, DataTypesUsageChecks *checks)
```
## Detailed Description
This function performs comprehensive data type usage validation during PostgreSQL cluster upgrades. It executes a series of configurable checks to identify columns that use data types with inconsistent on-disk representations across PostgreSQL server versions. The function uses a recursive Common Table Expression (CTE) to handle nested type dependencies including domains, arrays, composite types, and ranges that may wrap the problematic base types.

For each check that applies to the current cluster version, the function connects to every database and searches for stored columns in tables, materialized views, and indexes (but not regular views since they don't involve storage). When problematic columns are found, detailed reports are written to specified output files and the upgrade process is terminated with a fatal error.

The checks are driven by a DataTypesUsageChecks structure array that defines the metadata, SQL queries, version thresholds, and output files for each validation.

## Parameters / Member Variables
- : Pointer to ClusterInfo structure containing database cluster information and connection details
- : Array of DataTypesUsageChecks structures defining the validation rules, each containing status messages, base queries, report filenames, and version hooks

## Dependencies
- Functions called/Symbols referenced:
  - [prep_status](../p/prep_status.md)
  - [pg_malloc0](../p/pg_malloc0.md)
  - [connectToServer](connectToServer.md)
  - [executeQueryOrDie](../e/executeQueryOrDie.md)
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [pg_log](../p/pg_log.md)
  - fopen_priv
  - [PQfinish](../P/PQfinish.md)
  - [pg_free](../p/pg_free.md)
  - [check_ok](check_ok.md)
- Called from (representative examples):
  - [check_and_dump_old_cluster](check_and_dump_old_cluster.md)

## Notes and Other Information
- Uses recursive CTE queries to handle complex type hierarchies including domains, arrays, composite types, and ranges
- Excludes temporary tables (pg_temp_*, pg_toast_temp_*) and system catalogs from checks
- Searches only stored relations (tables, materialized views, indexes) and skips views
- Terminates the upgrade process with pg_fatal() if any problematic data types are detected
- Supports version-specific checks through threshold_version and version_hook mechanisms
- Reports are appended to output files to handle findings across multiple databases