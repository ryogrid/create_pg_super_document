# PgStat_Kind

## Location
src/include/pgstat.h: 54 - 55

## Overview
PgStat_Kind is an enumeration that defines the different types of statistics entries in PostgreSQL's statistics collection system.

## Definition


## Detailed Description
PgStat_Kind categorizes different types of statistics that PostgreSQL collects and maintains. The enumeration is divided into two main categories: statistics for variable-numbered objects (like databases, tables, functions) and statistics for fixed-numbered objects (like background processes). This classification helps the statistics system organize and manage different types of performance and activity data efficiently.

## Parameters / Member Variables
- : Invalid/uninitialized value (0) to catch zero-initialized data
- : Database-wide statistics tracking
- : Per-table statistics for relations
- : Per-function statistics for function calls
- : Statistics for replication slots
- : Statistics for logical replication subscriptions
- : Statistics for the WAL archiver process
- : Statistics for the background writer process
- : Statistics for the checkpointer process
- : I/O operation statistics
- : Simple LRU cache statistics
- : Write-Ahead Logging statistics

## Dependencies
- Functions called/Symbols referenced:
  - Used throughout the statistics system as a type identifier
- Called from (representative examples):
  - [pgstat_reset](../p/pgstat_reset.md)
  - [pgstat_fetch_entry](../p/pgstat_fetch_entry.md)
  - [pgstat_get_kind_info](../p/pgstat_get_kind_info.md)
  - [pgstat_build_snapshot](../p/pgstat_build_snapshot.md)
  - pgstat_init_entry

## Notes and Other Information
- The enumeration includes helper macros: PGSTAT_KIND_FIRST_VALID, PGSTAT_KIND_LAST, and PGSTAT_NUM_KINDS
- The invalid value is explicitly set to 0 to help detect uninitialized statistics entries
- The distinction between variable-numbered and fixed-numbered objects affects how statistics are stored and accessed
- This type is fundamental to PostgreSQL's statistics collection infrastructure, appearing in hash keys and entry management functions