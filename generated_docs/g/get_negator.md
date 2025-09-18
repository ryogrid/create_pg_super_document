# get_negator

## Location
src/backend/utils/cache/lsyscache.c: 1533 - 1556

## Overview
Returns the corresponding negator operator for a given operator OID, or InvalidOid if the operator has no negator or doesn't exist.

## Definition
```c
Oid get_negator(Oid opno)
```

## Detailed Description
This function retrieves the negator operator for a given operator from the PostgreSQL system catalog. A negator operator is an operator that produces the opposite logical result of the original operator. For example, the negator of "<" is ">=", and the negator of "=" is "<>".

The function performs a system catalog lookup in pg_operator using the provided operator OID. If the operator exists, it extracts the oprnegate field which contains the OID of the negator operator. If no operator is found with the given OID, it returns InvalidOid.

## Parameters / Member Variables
- `opno`: The OID of the operator for which to find the negator

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (macro to extract struct from tuple)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache cleanup)
  - Form_pg_operator (operator catalog structure)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) (OID to Datum conversion)
  - InvalidOid (invalid OID constant)

- Called from (representative examples):
  - [negate_clause](../n/negate_clause.md) (logical negation of clauses)
  - [operator_same_subexprs_proof](../o/operator_same_subexprs_proof.md) (predicate testing)
  - [convert_saop_to_hashed_saop_walker](../c/convert_saop_to_hashed_saop_walker.md) (expression transformation)
  - [match_clause_to_partition_key](../m/match_clause_to_partition_key.md) (partition pruning)
  - [neqjoinsel](../n/neqjoinsel.md) (selectivity estimation for inequality joins)

## Notes and Other Information
- This function is part of the lsyscache.c module which provides cached access to system catalog information
- The function returns InvalidOid (0) if the operator doesn't exist or has no defined negator
- Negator relationships are crucial for query optimization, particularly in predicate negation and constraint elimination
- The oprnegate field in pg_operator stores the OID of the negator operator, or 0 if none exists
- This function is frequently used in the query optimizer for logical transformations like NOT elimination
- Common negator pairs include: = and <>, < and >=, <= and >, etc.