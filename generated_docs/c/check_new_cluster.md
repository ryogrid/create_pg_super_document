# check_new_cluster

## Location
src/bin/pg_upgrade/check.c: 686 - 721

## Overview
Validates the new PostgreSQL cluster configuration and readiness for upgrade, including data transfer method verification, emptiness checks, and logical replication setup validation.

## Definition


## Detailed Description
This function performs comprehensive validation of the target (new) PostgreSQL cluster before proceeding with the pg_upgrade process. It first extracts database and slot information, then verifies that the new cluster is empty and ready to receive data. The function validates the availability of required libraries and tests the selected data transfer method (clone, copy, copy_file_range, or hard link) to ensure compatibility with the system.

Additionally, it performs standard checks such as verifying the installation user permissions, checking for prepared transactions that would block the upgrade, validating tablespace directory configurations, and ensuring proper setup of logical replication slots and subscriptions in the new cluster.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [get_db_rel_and_slot_infos](../g/get_db_rel_and_slot_infos.md)
  - [check_new_cluster_is_empty](check_new_cluster_is_empty.md)
  - [check_loadable_libraries](check_loadable_libraries.md)
  - [check_file_clone](check_file_clone.md)
  - [check_copy_file_range](check_copy_file_range.md)
  - [check_hard_link](check_hard_link.md)
  - [check_is_install_user](check_is_install_user.md)
  - [check_for_prepared_transactions](check_for_prepared_transactions.md)
  - [check_for_new_tablespace_dir](check_for_new_tablespace_dir.md)
  - [check_new_cluster_logical_replication_slots](check_new_cluster_logical_replication_slots.md)
  - [check_new_cluster_subscription_configuration](check_new_cluster_subscription_configuration.md)
- Called from (representative examples):
  - [main](../m/main.md)

## Dependencies
- Functions called/Symbols referenced:
  - [get_db_rel_and_slot_infos](../g/get_db_rel_and_slot_infos.md)
  - [check_new_cluster_is_empty](check_new_cluster_is_empty.md)
  - [check_loadable_libraries](check_loadable_libraries.md)
  - [check_file_clone](check_file_clone.md)
  - [check_copy_file_range](check_copy_file_range.md)  
  - [check_hard_link](check_hard_link.md)
  - [check_is_install_user](check_is_install_user.md)
  - [check_for_prepared_transactions](check_for_prepared_transactions.md)
  - [check_for_new_tablespace_dir](check_for_new_tablespace_dir.md)
  - [check_new_cluster_logical_replication_slots](check_new_cluster_logical_replication_slots.md)
  - [check_new_cluster_subscription_configuration](check_new_cluster_subscription_configuration.md)
- Called from (representative examples):
  - [main](../m/main.md)

## Notes and Other Information
- Operates on the global new_cluster variable
- Validates data transfer methods: TRANSFER_MODE_CLONE, TRANSFER_MODE_COPY, TRANSFER_MODE_COPY_FILE_RANGE, TRANSFER_MODE_LINK
- Uses switch statement to test only the selected transfer mode for efficiency
- TRANSFER_MODE_COPY requires no additional validation (break without checks)
- Ensures new cluster is completely empty before upgrade begins
- Validates logical replication components for clusters that support them
- Function has external linkage and can be called from other compilation units
- Essential prerequisite check before any data migration begins