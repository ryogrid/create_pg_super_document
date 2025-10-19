# set_tablespace_directory_suffix

## Location
[src/bin/pg_upgrade/tablespace.c:103-111](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/tablespace.c#L103-L111)

## Overview
Sets the version-specific directory suffix for tablespace paths in a PostgreSQL cluster during pg_upgrade operations.

## Definition
static void set_tablespace_directory_suffix(ClusterInfo *cluster)

## Detailed Description
The set_tablespace_directory_suffix function constructs a version-specific directory suffix that is used to organize tablespace data by PostgreSQL version and system catalog version. This function is critical for pg_upgrade operations as it ensures that different versions of PostgreSQL can coexist and be properly identified within the same tablespace directory structure.

The function creates a suffix in the format "/PG_{major_version}_{catalog_version}" where:
- major_version_str is the PostgreSQL major version (e.g., "13", "14", "15")  
- cat_ver is the system catalog version number from the cluster's control data

This suffix is used to create version-specific subdirectories within tablespace paths, allowing multiple PostgreSQL versions to share the same base tablespace location without conflicts. The leading slash ensures that the suffix starts a new directory level in the filesystem hierarchy.

## Parameters / Member Variables
- : Pointer to a ClusterInfo structure containing cluster metadata including:
  - : The major version string of the PostgreSQL cluster
  - : The system catalog version number
  - : Output field that will be set to the constructed suffix

## Dependencies
- Functions called/Symbols referenced:
  - [psprintf](../p/psprintf.md)
- Called from (representative examples):
  - [init_tablespaces](../i/init_tablespaces.md) (src/bin/pg_upgrade/tablespace.c:23)
  - [init_tablespaces](../i/init_tablespaces.md) (src/bin/pg_upgrade/tablespace.c:24)

## Notes and Other Information
- This is a static function, only accessible within the tablespace.c compilation unit
- The function uses psprintf to allocate and format the suffix string dynamically
- The constructed suffix is stored in cluster->tablespace_suffix for later use
- This versioning scheme prevents data corruption when multiple PostgreSQL versions access the same tablespace base directory
- The suffix is essential for the safety checks performed in init_tablespaces() to prevent upgrades between clusters with identical catalog versions
- Memory for the suffix string is managed by psprintf and should be freed appropriately

## Simplified Source

```c
static void set_tablespace_directory_suffix(ClusterInfo *cluster) {
    // Create version-specific directory suffix: /PG_{version}_{catalog_version}
    // This allows multiple PostgreSQL versions to coexist in same tablespace
    cluster->tablespace_suffix = psprintf("/PG_%s_%d",
                                          cluster->major_version_str,
                                          cluster->controldata.cat_ver);
}
```