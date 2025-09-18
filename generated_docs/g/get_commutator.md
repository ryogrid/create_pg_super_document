# get_commutator

## Location
src/backend/utils/cache/lsyscache.c: 1509 - 1532

## Overview
Returns the corresponding commutator operator for a given operator OID, or InvalidOid if the operator has no commutator or doesn't exist.

## Definition


## Detailed Description
This function retrieves the commutator operator for a given operator from the PostgreSQL system catalog. A commutator operator is an operator that produces the same result when its operands are swapped. For example, the commutator of "<" is ">", and the commutator of "=" is "=" itself.

The function performs a system catalog lookup in pg_operator using the provided operator OID. If the operator exists, it extracts the oprcom field which contains the OID of the commutator operator. If no operator is found with the given OID, it returns InvalidOid.

## Parameters / Member Variables
- `opno`: The OID of the operator for which to find the commutator

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1 (system cache lookup)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (macro to extract struct from tuple)
  - ReleaseSysCache (cache cleanup)
  - Form_pg_operator (operator catalog structure)
  - ObjectIdGetDatum (OID to Datum conversion)
  - InvalidOid (invalid OID constant)

- Called from (representative examples):
  - CommuteOpExpr (expression commutation)
  - operator_predicate_proof (predicate testing)
  - match_opclause_to_indexcol (index optimization)
  - compute_semijoin_info (join planning)
  - eqjoinsel (selectivity estimation)

## Notes and Other Information
- This function is part of the lsyscache.c module which provides cached access to system catalog information
- The function returns InvalidOid (0) if the operator doesn't exist or has no defined commutator
- Commutator relationships are essential for query optimization, allowing the optimizer to rewrite expressions in equivalent forms
- The oprcom field in pg_operator stores the OID of the commutator operator, or 0 if none exists
- This function is frequently used in the query optimizer for expression rewriting and index matching