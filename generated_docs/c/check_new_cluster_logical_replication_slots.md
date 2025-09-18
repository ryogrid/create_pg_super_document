# check_new_cluster_logical_replication_slots

## Location
src/bin/pg_upgrade/check.c: 1812 - 1884

## Overview
Validates that the new PostgreSQL cluster is properly configured to receive logical replication slots during an upgrade, ensuring no existing slots conflict and that required parameters are set correctly.

## Definition
```c
static void check_new_cluster_logical_replication_slots(void)
```

## Detailed Description
This function verifies that the new PostgreSQL cluster is ready to accept logical replication slots that will be migrated from the old cluster. It performs comprehensive validation to ensure the migration will succeed without conflicts or configuration issues. The function only operates when upgrading from PostgreSQL version 17 or later, as logical slot migration was introduced in that version.

The function performs the following validations:
- Checks if the old cluster has any logical replication slots to migrate
- Ensures the new cluster has no existing logical replication slots that would conflict
- Validates that wal_level is set to "logical" on the new cluster
- Verifies that max_replication_slots is sufficient to accommodate all slots from the old cluster

## Parameters / Member Variables
None - this function takes no parameters and operates on global cluster information.

## Dependencies
- Functions called/Symbols referenced:
  - GET_MAJOR_VERSION
  - count_old_cluster_logical_slots
  - connectToServer
  - prep_status
  - executeQueryOrDie
  - PQclear
  - PQfinish
  - check_ok
- Called from:
  - check_new_cluster

## Notes and Other Information
- This is a static function specific to pg_upgrade functionality
- Only operates when upgrading from PostgreSQL 17 or later (logical slot migration support)
- Terminates upgrade if the new cluster has any existing logical replication slots
- Requires wal_level to be set to "logical" on the new cluster
- Validates that max_replication_slots setting can accommodate all slots from the old cluster
- Part of PostgreSQL's upgrade validation for logical replication slot migration
- Returns early if the old cluster has no logical slots to migrate
- Ensures configuration compatibility before attempting slot migration