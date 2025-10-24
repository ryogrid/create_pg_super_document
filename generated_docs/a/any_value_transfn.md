# any_value_transfn

## Location
[src/backend/utils/adt/misc.c:1121-1124](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/misc.c#L1121-L1124)

## Overview
The transition function for PostgreSQL's ANY_VALUE aggregate that simply returns the first non-null input value encountered.

## Definition

```c
Datum
any_value_transfn(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the transition function for the ANY_VALUE aggregate in PostgreSQL. The ANY_VALUE aggregate is a non-deterministic aggregate function that returns an arbitrary (but not random) value from the set of input values. The transition function implements the core logic by simply returning the current state value, which effectively means it keeps the first non-null value it encounters during aggregation.

The implementation is intentionally simple - it just returns the first argument (the current transition state) unchanged. This behavior makes ANY_VALUE useful in situations where you need to pick any representative value from a group, particularly in GROUP BY queries where you need to select a column that's not in the GROUP BY clause but know that all values in the group are the same (or you don't care which specific value is returned).

## Parameters / Member Variables
- : The current transition state (carries forward the selected value)
- : The new input value (typically ignored since we keep the first value)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_DATUM (to extract the current state argument)
  - PG_RETURN_DATUM (to return the unchanged state value)
- Called from (representative examples):
  - PostgreSQL's aggregate execution engine during ANY_VALUE aggregate processing
  - SQL queries using ANY_VALUE() aggregate function

## Notes and Other Information
- Part of PostgreSQL's aggregate function framework
- Implements the "take first value" strategy for the ANY_VALUE aggregate
- The simplicity of this function reflects the straightforward semantics of ANY_VALUE
- Located in src/backend/utils/adt/misc.c:1121-1124
- Used internally by the aggregate execution system, not typically called directly by user code
- The actual ANY_VALUE aggregate behavior depends on this transition function combined with the aggregate definition in the system catalogs

## Simplified Source

```c
Datum
any_value_transfn(PG_FUNCTION_ARGS)
{
    // Return the current state unchanged (keeps first non-null value)
    PG_RETURN_DATUM(PG_GETARG_DATUM(0));
}
```