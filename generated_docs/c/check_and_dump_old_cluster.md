# check_and_dump_old_cluster

## Location
[src/bin/pg_upgrade/check.c:577-685](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/check.c#L577-L685)

## Overview
Performs comprehensive validation and analysis of the old PostgreSQL cluster during upgrade, running version-specific compatibility checks and optionally generating a schema dump for migration.

## Definition

```c
void
check_and_dump_old_cluster(bool live_check)
```
## Detailed Description
This function orchestrates the complete validation process for the old PostgreSQL cluster during pg_upgrade operations. It starts the old cluster's postmaster (if not performing live checks), extracts database and table information, and executes a series of version-specific compatibility checks to ensure the cluster can be safely upgraded.

The function performs checks for various incompatibilities including prepared transactions, data type usage issues, encoding conversions, postfix operators, polymorphic functions, tables with OIDs, and role naming conflicts. For newer clusters (PG 17+), it also handles logical replication slots and subscriptions. Finally, if not running in check-only mode, it generates a complete schema dump of the old cluster before shutting down the postmaster.

## Parameters / Member Variables
- `live_check`: Boolean indicating whether checks are performed against a live running server (true) or by starting a temporary server instance (false)
## Dependencies
- Functions called/Symbols referenced:
  - [start_postmaster](../s/start_postmaster.md)
  - [get_db_rel_and_slot_infos](../g/get_db_rel_and_slot_infos.md)
  - [init_tablespaces](../i/init_tablespaces.md)
  - [get_loadable_libraries](../g/get_loadable_libraries.md)
  - [check_is_install_user](check_is_install_user.md)
  - [check_proper_datallowconn](check_proper_datallowconn.md)
  - [check_for_prepared_transactions](check_for_prepared_transactions.md)
  - [check_for_isn_and_int8_passing_mismatch](check_for_isn_and_int8_passing_mismatch.md)
  - [check_old_cluster_for_valid_slots](check_old_cluster_for_valid_slots.md)
  - [get_subscription_count](../g/get_subscription_count.md)
  - [check_old_cluster_subscription_state](check_old_cluster_subscription_state.md)
  - [check_for_data_types_usage](check_for_data_types_usage.md)
  - [check_for_user_defined_encoding_conversions](check_for_user_defined_encoding_conversions.md)
  - [check_for_user_defined_postfix_ops](check_for_user_defined_postfix_ops.md)
  - [check_for_incompatible_polymorphics](check_for_incompatible_polymorphics.md)
  - [check_for_tables_with_oids](check_for_tables_with_oids.md)
  - [check_for_not_null_inheritance](check_for_not_null_inheritance.md)
  - [old_9_6_invalidate_hash_indexes](../o/old_9_6_invalidate_hash_indexes.md)
  - [check_for_pg_role_prefix](check_for_pg_role_prefix.md)
  - [generate_old_dump](../g/generate_old_dump.md)
  - [stop_postmaster](../s/stop_postmaster.md)
- Called from (representative examples):
  - [main](../m/main.md)

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

## Simplified Source

```c
void check_and_dump_old_cluster(bool live_check)
{
    // Start old cluster postmaster if not live checking
    if (!live_check)
        start_postmaster(&old_cluster, true);

    // Extract database, table, and logical slot information
    get_db_rel_and_slot_infos(&old_cluster, live_check);
    init_tablespaces();
    get_loadable_libraries();

    // Run core compatibility checks
    check_is_install_user(&old_cluster);
    check_proper_datallowconn(&old_cluster);
    check_for_prepared_transactions(&old_cluster);
    check_for_isn_and_int8_passing_mismatch(&old_cluster);

    // PG 17+ specific checks for logical replication
    if (GET_MAJOR_VERSION(old_cluster.major_version) >= 1700) {
        check_old_cluster_for_valid_slots(live_check);
        get_subscription_count(&old_cluster);
        check_old_cluster_subscription_state();
    }

    // Check for problematic data types
    check_for_data_types_usage(&old_cluster, data_types_usage_checks);

    // Version-specific incompatibility checks
    if (GET_MAJOR_VERSION(old_cluster.major_version) <= 1300) {
        // PG 14 changed encoding conversion function signatures
        check_for_user_defined_encoding_conversions(&old_cluster);

        // PG 14 removed support for postfix operators
        check_for_user_defined_postfix_ops(&old_cluster);

        // PG 14 changed polymorphic function behavior
        check_for_incompatible_polymorphics(&old_cluster);
    }

    if (GET_MAJOR_VERSION(old_cluster.major_version) <= 1100) {
        // PG 12 removed support for WITH OIDS tables
        check_for_tables_with_oids(&old_cluster);
    }

    // PG 18 changed inheritance not-null constraint handling
    check_for_not_null_inheritance(&old_cluster);

    if (GET_MAJOR_VERSION(old_cluster.major_version) <= 906) {
        // PG 10 invalidated hash indexes from earlier versions
        if (user_opts.check)
            old_9_6_invalidate_hash_indexes(&old_cluster, true);
    }

    if (GET_MAJOR_VERSION(old_cluster.major_version) <= 905) {
        // PG 9.6 restricted pg_ role prefixes
        check_for_pg_role_prefix(&old_cluster);
    }

    // Generate schema dump if not just checking
    if (!user_opts.check)
        generate_old_dump();

    // Stop postmaster if we started it
    if (!live_check)
        stop_postmaster(false);
}
```