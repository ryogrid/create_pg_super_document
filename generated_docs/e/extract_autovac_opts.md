# extract_autovac_opts

## Location
src/backend/postmaster/autovacuum.c: 2702 - 2731

## Overview
extract_autovac_opts extracts and returns a copy of the AutoVacOpts portion of a relation's reloptions from its pg_class tuple.

## Definition
```c
static AutoVacOpts *extract_autovac_opts(HeapTuple tup, TupleDesc pg_class_desc)
```

## Detailed Description
This function serves as a utility to extract autovacuum-specific configuration options from a relation's storage parameters (reloptions). It processes the reloptions bytea field from a pg_class tuple and extracts the autovacuum portion, which contains settings like vacuum thresholds, scale factors, and cost parameters that override system-wide autovacuum settings for specific tables.

The function handles the case where no autovacuum options are explicitly set by returning NULL. When options are present, it creates a palloc'd copy of the AutoVacOpts structure, ensuring the caller owns the memory and is responsible for freeing it.

## Parameters / Member Variables
- `tup`: HeapTuple from pg_class containing the relation's catalog information
- `pg_class_desc`: TupleDesc describing the structure of the pg_class relation
- Returns: AutoVacOpts pointer containing extracted options, or NULL if no options are set

## Dependencies
- Functions called/Symbols referenced:
  - [extractRelOptions](extractRelOptions.md) (extracts reloptions bytea from tuple)
  - [palloc](../p/palloc.md) (memory allocation)
  - memcpy (memory copying)
  - [pfree](../p/pfree.md) (memory deallocation)
  - Form_pg_class/GETSTRUCT (tuple structure access)
- Called from (representative examples):
  - [do_autovacuum](../d/do_autovacuum.md) (during table scanning for autovacuum candidates)
  - [table_recheck_autovac](../t/table_recheck_autovac.md) (when rechecking table maintenance needs)

## Notes and Other Information
- Only processes relations, materialized views, and TOAST tables (verified by assertions)
- Safe to call without relation locks since pg_class doesn't have a TOAST table
- Caller is responsible for freeing the returned AutoVacOpts structure
- Returns NULL when no autovacuum-specific reloptions are configured
- The extracted options include parameters like vacuum_threshold, vacuum_scale_factor, analyze_threshold, analyze_scale_factor, and vacuum cost settings
- Memory allocation uses palloc, making the result subject to PostgreSQL's memory context management