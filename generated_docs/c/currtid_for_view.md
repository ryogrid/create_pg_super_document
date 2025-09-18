# currtid_for_view

## Location
src/backend/utils/adt/tid.c: 336 - 407

## Overview
A specialized function that handles current tuple identifier (CTID) operations for views by analyzing their rule definitions and delegating to the underlying base relations.

## Definition
```c
static ItemPointer currtid_for_view(Relation viewrel, ItemPointer tid)
```

## Detailed Description
The `currtid_for_view` function handles CTID operations specifically for PostgreSQL views. Since views are virtual tables that don't store data directly, this function must analyze the view's definition to find the underlying base relation that actually contains the tuple data.

The function works by:
1. Examining the view's tuple descriptor to locate a CTID column
2. Validating that the CTID column has the correct TID type
3. Analyzing the view's rewrite rules to find the SELECT rule
4. Examining the target list of the SELECT query to identify which base relation the CTID refers to
5. Opening the base relation and delegating the actual CTID lookup to `currtid_internal`

The implementation includes several validation checks to ensure the view is properly structured with exactly one SELECT rule and a valid CTID column that maps to a base relation's system CTID column.

## Parameters / Member Variables
- `viewrel`: The view relation for which to handle the CTID operation
- `tid`: Pointer to the tuple identifier to look up in the underlying base relation

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetDescr
  - TupleDescAttr
  - NameStr
  - elog
  - list_length
  - linitial
  - get_tle_by_resno
  - IsA
  - IS_SPECIAL_VARNO
  - rt_fetch
  - table_open
  - currtid_internal
  - table_close
- Called from (representative examples):
  - currtid_internal

## Notes and Other Information
- This is a static function with internal linkage, used only within the TID utilities module
- The function performs extensive validation to ensure the view is compatible with CTID operations
- Views must have exactly one SELECT rule to be supported
- The CTID column in the view must map directly to a base relation's system CTID column (SelfItemPointerAttributeNumber)
- The function uses AccessShareLock when opening the base relation to ensure safe concurrent access
- Error handling is comprehensive, with specific error messages for various failure conditions such as missing CTID columns, invalid CTID types, missing rules, or unsupported view structures
- The implementation supports the PostgreSQL rule system and query rewriting mechanism for views