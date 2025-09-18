# recheck_relation_needs_vacanalyze

## Location
src/backend/postmaster/autovacuum.c: 2877 - 2941

## Overview
recheck_relation_needs_vacanalyze fetches fresh statistics for a relation and determines whether it needs vacuum or analyze operations.

## Definition
```c
static void recheck_relation_needs_vacanalyze(Oid relid,
                                             AutoVacOpts *avopts,
                                             Form_pg_class classForm,
                                             int effective_multixact_freeze_max_age,
                                             bool *dovacuum,
                                             bool *doanalyze,
                                             bool *wraparound)
```

## Detailed Description
This function serves as a thin wrapper around the core relation_needs_vacanalyze function, specifically designed for rechecking operations. Its primary responsibility is to fetch the most current statistics for a relation and delegate the actual vacuum/analyze decision logic to relation_needs_vacanalyze.

The function ensures that decisions are based on the most up-to-date statistical information by fetching fresh pgstat data. It also implements TOAST table-specific logic by explicitly disabling analyze operations for TOAST tables, since TOAST tables are not analyzed independently but rather as part of their parent table operations.

## Parameters / Member Variables
- `relid`: OID of the relation to check
- `avopts`: Pointer to AutoVacOpts containing table-specific autovacuum settings, or NULL for defaults
- `classForm`: Form_pg_class structure containing relation metadata from pg_class
- `effective_multixact_freeze_max_age`: Effective freeze age threshold for multixact IDs
- `dovacuum`: Output parameter - set to true if vacuum is needed
- `doanalyze`: Output parameter - set to true if analyze is needed
- `wraparound`: Output parameter - set to true if this is a wraparound prevention vacuum

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_fetch_stat_tabentry_ext](../p/pgstat_fetch_stat_tabentry_ext.md) (fetch current statistics for the relation)
  - [relation_needs_vacanalyze](relation_needs_vacanalyze.md) (core logic for determining maintenance needs)
  - [pfree](../p/pfree.md) (memory management for statistics entry)
  - RELKIND_TOASTVALUE (constant for identifying TOAST tables)
- Called from (representative examples):
  - [table_recheck_autovac](../t/table_recheck_autovac.md) (during table maintenance need validation)

## Notes and Other Information
- This function is specifically designed as a subroutine for table_recheck_autovac
- Explicitly disables analyze operations for TOAST tables since they don't require independent analysis
- Properly manages memory by freeing the fetched statistics entry to prevent leakage
- The function ensures fresh statistics are used for decision making, which is critical for avoiding unnecessary work
- Delegates all actual decision logic to relation_needs_vacanalyze, maintaining code reuse and consistency
- The distinction from relation_needs_vacanalyze is that this function fetches fresh statistics rather than using pre-fetched ones