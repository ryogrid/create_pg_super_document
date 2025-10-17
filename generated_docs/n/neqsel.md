# neqsel

## Location
[src/backend/utils/adt/selfuncs.c:558-580](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L558-L580)

## Overview
The neqsel function provides selectivity estimation for inequality operators ("!=") and other operators with comparable selectivity behavior across any PostgreSQL data types.

## Definition

```c
Datum
neqsel(PG_FUNCTION_ARGS)
```
## Detailed Description
The neqsel function serves as the primary entry point for selectivity estimation of inequality operations in PostgreSQL's query planner. It acts as a simple wrapper function that delegates the actual computation to eqsel_internal with the negate flag set to true. This design allows neqsel to leverage the same sophisticated estimation logic used for equality operations while applying the appropriate mathematical transformation for inequality.

Like eqsel, this function supports operators that are not strict inequality ("!=") but have comparable selectivity behavior. The function works with any data types and handles cases where the left and right operand data types may differ, making it a versatile component in PostgreSQL's cost-based query optimization framework.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [eqsel_internal](../e/eqsel_internal.md)
- Called from (representative examples):
  - Used by PostgreSQL's query planner for selectivity estimation
  - Referenced in operator catalog entries for inequality operators

## Notes and Other Information
- Located in src/backend/utils/adt/selfuncs.c:558-580
- Returns a float8 value representing the estimated selectivity
- The function passes 'true' as the second parameter to eqsel_internal, enabling inequality processing
- Uses the formula: 1.0 - equality_selectivity - nullfrac for selectivity calculation
- Part of PostgreSQL's selectivity function framework used by the query optimizer for cost-based planning
- Complements eqsel by providing the inverse selectivity estimation for the same operator families

## Simplified Source

```c
Datum neqsel(PG_FUNCTION_ARGS) {
    // Calculate inequality selectivity by delegating to eqsel_internal
    // with negate flag set to true
    return (float8) eqsel_internal(fcinfo, true);
}
```