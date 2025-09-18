# get_oprrest

## Location
[src/backend/utils/cache/lsyscache.c:1557-1580](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L1557-L1580)

## Overview
Returns the procedure ID (function OID) for computing the restriction selectivity of an operator, or InvalidOid if no selectivity function is defined.

## Definition
```c
RegProcedure get_oprrest(Oid opno)
```

## Detailed Description
This function retrieves the restriction selectivity estimation function for a given operator from the PostgreSQL system catalog. Restriction selectivity functions estimate how many rows will satisfy a WHERE clause condition involving the operator. This information is crucial for the query optimizer to generate efficient execution plans.

The function performs a system catalog lookup in pg_operator using the provided operator OID. If the operator exists, it extracts the oprrest field which contains the OID of the selectivity estimation function. If no operator is found with the given OID, it returns InvalidOid.

Restriction selectivity is used for conditions like "column op constant" where the operator restricts the result set based on a single table's column values.

## Parameters / Member Variables
- `opno`: The OID of the operator for which to find the restriction selectivity function

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (macro to extract struct from tuple)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache cleanup)
  - Form_pg_operator (operator catalog structure)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) (OID to Datum conversion)
  - InvalidOid (invalid OID constant)
  - RegProcedure (procedure identifier type)

- Called from (representative examples):
  - [clauselist_selectivity_ext](../c/clauselist_selectivity_ext.md) (selectivity estimation for clause lists)
  - [restriction_selectivity](../r/restriction_selectivity.md) (core selectivity estimation)
  - [dependency_is_compatible_clause](../d/dependency_is_compatible_clause.md) (statistics dependency analysis)
  - [statext_is_compatible_clause_internal](../s/statext_is_compatible_clause_internal.md) (extended statistics compatibility)
  - scalararraysel (scalar array selectivity estimation)

## Notes and Other Information
- This function is part of the lsyscache.c module which provides cached access to system catalog information
- The function returns InvalidOid (0) if the operator doesn't exist or has no defined restriction selectivity function
- Restriction selectivity functions typically return a value between 0.0 and 1.0 representing the fraction of rows expected to satisfy the condition
- The oprrest field in pg_operator stores the OID of the selectivity estimation function
- Common selectivity functions include eqsel (equality), scalarltsel (less than), neqsel (not equal), etc.
- This function is essential for cost-based query optimization, helping the planner estimate cardinalities and choose optimal join orders and access methods
- If no specific selectivity function is provided, PostgreSQL uses default estimation methods