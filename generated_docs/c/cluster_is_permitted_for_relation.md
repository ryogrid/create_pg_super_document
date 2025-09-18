# cluster_is_permitted_for_relation

## Location
src/backend/commands/cluster.c: 1738 - 1747

## Overview
Checks whether a specified user has the necessary privileges to perform CLUSTER operations on a given relation, emitting a warning if permission is denied.

## Definition
static bool cluster_is_permitted_for_relation(Oid relid, Oid userid)

## Detailed Description
This function serves as a permission checker for clustering operations by verifying that the specified user has ACL_MAINTAIN privileges on the given relation. The ACL_MAINTAIN privilege is required for clustering because the operation involves reorganizing table data, which is considered a maintenance operation. If the user lacks the required privileges, the function emits a WARNING message indicating that the relation will be skipped, but continues processing rather than failing entirely.

The function uses the PostgreSQL access control system via pg_class_aclcheck() to determine if the user has sufficient privileges. This allows for graceful handling of permission issues during batch clustering operations where some tables may be accessible while others are not.

## Parameters / Member Variables
- : OID of the relation to check clustering permissions for
- : OID of the user whose permissions should be checked

## Dependencies
- Functions called/Symbols referenced:
  - [pg_class_aclcheck](../p/pg_class_aclcheck.md)
  - ereport
  - [get_rel_name](../g/get_rel_name.md)
- Called from (representative examples):
  - [cluster_rel](cluster_rel.md)
  - [get_tables_to_cluster](../g/get_tables_to_cluster.md)
  - [get_tables_to_cluster_partitioned](../g/get_tables_to_cluster_partitioned.md)

## Notes and Other Information
- This is a static function internal to cluster.c
- Requires ACL_MAINTAIN privilege level for clustering operations
- Emits WARNING rather than ERROR to allow batch operations to continue
- Uses get_rel_name() to provide helpful relation name in warning messages
- Returns true if permission is granted, false if denied
- Essential for security in multi-user environments where not all users should be able to cluster all tables