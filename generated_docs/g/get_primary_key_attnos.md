# get_primary_key_attnos

## Location
src/backend/catalog/pg_constraint.c: 1149 - 1234

## Overview
Identifies the columns that comprise a relation's primary key and returns them as a bitmap set of column attribute numbers.

## Definition
```c
Bitmapset *get_primary_key_attnos(Oid relid, bool deferrableOk, Oid *constraintOid)
```

## Detailed Description
This function searches the pg_constraint system catalog for the primary key constraint of a specified relation. When found, it extracts the column attribute numbers from the constraint's conkey array and returns them as a Bitmapset, with attribute numbers offset by FirstLowInvalidHeapAttributeNumber to allow representation of system columns.

The function specifically looks for constraints of type CONSTRAINT_PRIMARY and can optionally handle deferrable primary keys based on the deferrableOk parameter. Since there can be at most one primary key per table, the function stops searching once it finds a matching primary key constraint.

## Parameters / Member Variables
- `relid`: OID of the relation to search for a primary key
- `deferrableOk`: If false, ignores deferrable primary key constraints; if true, accepts both deferrable and immediate constraints
- `constraintOid`: Output parameter that receives the OID of the primary key constraint, or InvalidOid if not found

## Dependencies
- Functions called/Symbols referenced:
  - [SysScanDesc](../S/SysScanDesc.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - Form_pg_constraint
  - CONSTRAINT_PRIMARY
  - [heap_getattr](../h/heap_getattr.md)
  - DatumGetArrayTypeP
  - ARR_DIMS, ARR_NDIM, ARR_HASNULL, ARR_ELEMTYPE, ARR_DATA_PTR
  - [bms_add_member](../b/bms_add_member.md)
  - FirstLowInvalidHeapAttributeNumber
- Called from (representative examples):
  - [check_functional_grouping](../c/check_functional_grouping.md)
  - [remove_useless_groupby_columns](../r/remove_useless_groupby_columns.md)
  - [ConstraintCategory](../C/ConstraintCategory.md)

## Notes and Other Information
- Returns NULL if no primary key exists or if a deferrable primary key is found but deferrableOk is false
- The function validates that the conkey array is properly formatted (1-D smallint array without nulls)
- Attribute numbers in the returned Bitmapset are offset by FirstLowInvalidHeapAttributeNumber to handle system columns
- Uses AccessShareLock on pg_constraint for consistent reads
- Early termination optimization: stops searching after finding the primary key since there can only be one per table
- Used in query optimization contexts where knowledge of primary key columns helps with grouping and uniqueness analysis