# get_constraint_index

## Location
src/backend/utils/cache/lsyscache.c: 1113 - 1142

## Overview
Returns the OID of the underlying index for a given unique, primary-key, or exclusion constraint.

## Definition


## Detailed Description
This function retrieves the index OID that is "owned" by a specified constraint. It specifically handles unique, primary-key, and exclusion constraints, which are the constraint types that have associated indexes. The function ensures that only constraints that own their indexes are processed by checking the constraint type (contype), as some pg_constraint entries (like foreign-key constraints) may reference indexes they don't own.

The function performs a system catalog lookup using the constraint OID and returns the associated index OID (conindid field) only for appropriate constraint types. If the constraint is not found or is of an inappropriate type, InvalidOid is returned.

## Parameters / Member Variables
- : The OID of the constraint for which to retrieve the associated index

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1
  - ObjectIdGetDatum
  - HeapTupleIsValid
  - GETSTRUCT
  - ReleaseSysCache
  - Form_pg_constraint
  - CONSTRAINT_UNIQUE
  - CONSTRAINT_PRIMARY
  - CONSTRAINT_EXCLUSION
  - InvalidOid

- Called from (representative examples):
  - RememberConstraintForRebuilding (src/backend/commands/tablecmds.c:13746)
  - ATPostAlterTypeParse (src/backend/commands/tablecmds.c:14130)
  - infer_arbiter_indexes (src/backend/optimizer/util/plancat.c:780)

## Notes and Other Information
- Only returns index OIDs for constraints that "own" their indexes (unique, primary-key, exclusion)
- Foreign-key constraints and other constraint types return InvalidOid
- The function uses the system cache (CONSTROID) for efficient constraint lookup
- Part of the lsyscache utility functions for PostgreSQL system catalog access