# get_relation_name

## Location
src/backend/utils/adt/ruleutils.c: 12803 - 12822

## Overview
A utility function that retrieves the unqualified name of a relation by its OID, with strict error handling that throws an error if the relation is not found.

## Definition


## Detailed Description
This function serves as a wrapper around the lower-level get_rel_name() function, providing more robust error handling. While get_rel_name() returns NULL for invalid OIDs, this function throws an ERROR instead, ensuring that calling code doesn't need to handle NULL returns. This makes it suitable for use in contexts where the relation is expected to exist, and a missing relation indicates a serious problem that should halt execution.

## Parameters / Member Variables
- `relid`: The OID of the relation whose name should be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - get_rel_name
  - elog
- Called from (representative examples):
  - pg_get_indexdef_worker
  - pg_get_statisticsobj_worker
  - pg_get_constraintdef_worker
  - get_rte_alias
  - NameHashEntry structure usage

## Notes and Other Information
- This is a static function local to ruleutils.c
- Provides fail-fast behavior for relation lookups where the relation must exist
- Used primarily in SQL rule generation and formatting contexts
- The returned string is allocated by the underlying get_rel_name() function
- Part of PostgreSQL's defensive programming practices for system catalog lookups