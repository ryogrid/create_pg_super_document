# check_and_dump_old_cluster

## Location
src/bin/pg_upgrade/check.c: 577 - 685

## Overview
Performs comprehensive validation and analysis of the old PostgreSQL cluster during upgrade, running version-specific compatibility checks and optionally generating a schema dump for migration.

## Definition


## Detailed Description
This function orchestrates the complete validation process for the old PostgreSQL cluster during pg_upgrade operations. It starts the old cluster's postmaster (if not performing live checks), extracts database and table information, and executes a series of version-specific compatibility checks to ensure the cluster can be safely upgraded.

The function performs checks for various incompatibilities including prepared transactions, data type usage issues, encoding conversions, postfix operators, polymorphic functions, tables with OIDs, and role naming conflicts. For newer clusters (PG 17+), it also handles logical replication slots and subscriptions. Finally, if not running in check-only mode, it generates a complete schema dump of the old cluster before shutting down the postmaster.

## Parameters / Member Variables
- : Boolean indicating whether checks are performed against a live running server (true) or by starting a temporary server instance (false)

## Dependencies
- Functions called/Symbols referenced:
  - start_postmaster
  - get_db_rel_and_slot_infos
  - init_tablespaces
  - get_loadable_libraries
  - check_is_install_user
  - check_proper_datallowconn
  - check_for_prepared_transactions
  - check_for_isn_and_int8_passing_mismatch
  - check_old_cluster_for_valid_slots
  - get_subscription_count
  - check_old_cluster_subscription_state
  - check_for_data_types_usage
  - check_for_user_defined_encoding_conversions
  - check_for_user_defined_postfix_ops
  - check_for_incompatible_polymorphics
  - check_for_tables_with_oids
  - check_for_not_null_inheritance
  - old_9_6_invalidate_hash_indexes
  - check_for_pg_role_prefix
  - generate_old_dump
  - stop_postmaster
- Called from (representative examples):
  - main

## Notes and Other Information
- Manages the old cluster's postmaster lifecycle (start/stop) unless performing live checks
- Executes version-specific checks using GET_MAJOR_VERSION() comparisons
- PG 17+ support includes logical replication slot and subscription migration
- Pre-PG 14 checks include encoding conversions and postfix operators
- Pre-PG 12 checks include tables with OIDs
- Pre-PG 10 includes hash index invalidation for check mode
- Pre-PG 9.6 includes role prefix validation
- Generates complete schema dump via pg_dumpall unless in check-only mode
- Global variables: old_cluster, user_opts, data_types_usage_checks
- Function has external linkage and can be called from other compilation units