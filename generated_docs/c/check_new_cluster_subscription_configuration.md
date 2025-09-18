# check_new_cluster_subscription_configuration

## Location
src/bin/pg_upgrade/check.c: 1885 - 1927

## Overview
Validates that the new PostgreSQL cluster has sufficient max_replication_slots configuration to accommodate all subscriptions being migrated from the old cluster.

## Definition
```c
static void check_new_cluster_subscription_configuration(void)
```

## Detailed Description
This function ensures that the new PostgreSQL cluster is properly configured to handle the migration of subscriptions from the old cluster. Since each subscription requires a replication origin to be created, the function validates that the max_replication_slots parameter is set high enough to accommodate all subscriptions being migrated. This check only applies to upgrades from PostgreSQL 17 or later, when subscription migration was introduced.

The function performs the following operations:
- Checks if subscription migration is supported (PostgreSQL 17+)
- Returns early if there are no subscriptions to migrate from the old cluster
- Connects to the new cluster's template1 database
- Queries the max_replication_slots setting from pg_settings
- Validates that the setting is sufficient for all subscriptions to be migrated

## Parameters / Member Variables
None - this function takes no parameters and operates on global cluster information.

## Dependencies
- Functions called/Symbols referenced:
  - GET_MAJOR_VERSION
  - prep_status
  - connectToServer
  - executeQueryOrDie
  - PQclear
  - PQfinish
  - check_ok
- Called from:
  - check_new_cluster

## Notes and Other Information
- This is a static function specific to pg_upgrade functionality
- Only operates when upgrading from PostgreSQL 17 or later (subscription migration support)
- Uses old_cluster.nsubs to determine the number of subscriptions requiring migration
- Terminates upgrade if max_replication_slots is insufficient for the subscription count
- Each subscription requires a replication origin, which consumes a replication slot
- Part of PostgreSQL's upgrade validation for logical replication subscription migration
- Returns early if there are no subscriptions to migrate, optimizing for common cases
- Ensures configuration compatibility before attempting subscription migration