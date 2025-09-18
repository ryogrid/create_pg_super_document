# get_oprjoin

## Location
src/backend/utils/cache/lsyscache.c: 1581 - 1607

## Overview
Returns the procedure ID (function OID) for computing the join selectivity of an operator, or InvalidOid if no join selectivity function is defined.

## Definition
```c
RegProcedure get_oprjoin(Oid opno)
```

## Detailed Description
This function retrieves the join selectivity estimation function for a given operator from the PostgreSQL system catalog. Join selectivity functions estimate how many rows will result from a join operation using the operator in the join condition. This information is essential for the query optimizer to evaluate different join strategies and choose the most efficient execution plan.

The function performs a system catalog lookup in pg_operator using the provided operator OID. If the operator exists, it extracts the oprjoin field which contains the OID of the join selectivity estimation function. If no operator is found with the given OID, it returns InvalidOid.

Join selectivity is used for conditions like "table1.column op table2.column" where the operator defines the relationship between columns from different tables in a join operation.

## Parameters / Member Variables
- `opno`: The OID of the operator for which to find the join selectivity function

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1 (system cache lookup)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (macro to extract struct from tuple)
  - ReleaseSysCache (cache cleanup)
  - Form_pg_operator (operator catalog structure)
  - ObjectIdGetDatum (OID to Datum conversion)
  - InvalidOid (invalid OID constant)
  - RegProcedure (procedure identifier type)

- Called from (representative examples):
  - join_selectivity (core join selectivity estimation)
  - scalararraysel (scalar array selectivity estimation)

## Notes and Other Information
- This function is part of the lsyscache.c module which provides cached access to system catalog information
- The function returns InvalidOid (0) if the operator doesn't exist or has no defined join selectivity function
- Join selectivity functions typically return a value between 0.0 and 1.0 representing the fraction of the cartesian product expected to satisfy the join condition
- The oprjoin field in pg_operator stores the OID of the join selectivity estimation function
- Common join selectivity functions include eqjoinsel (equality joins), neqjoinsel (inequality joins), scalarltjoinsel (less than joins), etc.
- This function is crucial for cost-based query optimization, particularly for choosing optimal join orders in multi-table queries
- Join selectivity estimation is more complex than restriction selectivity as it involves relationships between multiple tables
- If no specific join selectivity function is provided, PostgreSQL uses default estimation methods based on the operator type and available statistics