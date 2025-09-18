# get_relation_constraint_attnos

## Location
src/backend/catalog/pg_constraint.c: 954 - 1042

## Overview
Finds a constraint on a specified relation by name and returns the constrained columns as a bitmap set of column attribute numbers.

## Definition
```c
Bitmapset *get_relation_constraint_attnos(Oid relid, const char *conname, bool missing_ok, Oid *constraintOid)
```

## Detailed Description
This function searches the pg_constraint system catalog to find a constraint with the specified name on the given relation. It returns the attribute numbers of the constrained columns as a Bitmapset, with attribute numbers offset by FirstLowInvalidHeapAttributeNumber to allow representation of system columns. The function also returns the OID of the matching constraint through an output parameter.

The function performs a system catalog scan using three search keys: relation OID, constraint type (set to InvalidOid to match relation constraints), and constraint name. When a matching constraint is found, it extracts the conkey array which contains the attribute numbers of the constrained columns and converts them into a Bitmapset.

## Parameters / Member Variables
- `relid`: OID of the relation to search for constraints
- `conname`: Name of the constraint to find
- `missing_ok`: If false, raises an error when the constraint is not found; if true, returns NULL silently
- `constraintOid`: Output parameter that receives the OID of the found constraint, or InvalidOid if not found

## Dependencies
- Functions called/Symbols referenced:
  - SysScanDesc
  - CStringGetDatum
  - systable_beginscan
  - systable_getnext
  - Form_pg_constraint
  - heap_getattr
  - DatumGetArrayTypeP
  - ARR_DIMS, ARR_NDIM, ARR_HASNULL, ARR_ELEMTYPE, ARR_DATA_PTR
  - bms_add_member
  - FirstLowInvalidHeapAttributeNumber
  - get_rel_name
- Called from (representative examples):
  - transformOnConflictArbiter
  - ConstraintCategory

## Notes and Other Information
- The function accesses pg_constraint with AccessShareLock to ensure consistent reads
- Attribute numbers in the returned Bitmapset are offset by FirstLowInvalidHeapAttributeNumber to handle system columns
- The function validates that the conkey array is properly formatted (1-D smallint array without nulls)
- Only relation constraints are matched (contypid is set to InvalidOid in the search key)
- The function can handle missing constraints gracefully when missing_ok is true