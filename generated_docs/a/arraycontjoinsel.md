# arraycontjoinsel

## Location
[src/backend/utils/adt/array_selfuncs.c:321-336](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_selfuncs.c#L321-L336)

## Overview
Join selectivity function for array containment operators (@>, &&, <@) that currently provides only default selectivity estimates.

## Definition

```c
Datum
arraycontjoinsel(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is intended to estimate join selectivity for array containment operations when arrays from two different relations are compared. However, it is currently implemented as a stub function that simply returns the default selectivity for the given operator.

Unlike restriction selectivity (handled by arraycontsel), join selectivity estimation for array operations is more complex as it requires analyzing the relationship between arrays in different tables. The current implementation indicates this functionality is not yet fully developed in PostgreSQL.

## Parameters
Uses PostgreSQL's standard function argument interface:
- : Operator OID (only parameter currently used)

## Dependencies
- Functions called/Symbols referenced:
  - DEFAULT_SEL
- Called from:
  - (No direct references found - likely registered as operator join selectivity function)

## Notes and Other Information
- Currently implemented as a stub function
- Returns default selectivity regardless of actual join conditions
- Represents a potential area for future enhancement in PostgreSQL's query optimizer
- Part of PostgreSQL's extensible operator selectivity framework
- The comment indicates this is temporary implementation pending proper join selectivity analysis