# oprfuncid

## Location
[src/backend/parser/parse_oper.c:245-261](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_oper.c#L245-L261)

## Overview
Extracts the underlying function OID from an operator tuple, providing access to the function that implements the operator's behavior.

## Definition
```c
Oid oprfuncid(Operator op)
```

## Detailed Description
oprfuncid retrieves the OID of the function that implements an operator's actual computation logic. Every operator in PostgreSQL is backed by a function that performs the operation (e.g., int4plus for the + operator on integers). This function provides clean access to the oprcode field of the pg_operator catalog entry, which stores the implementing function's OID. This is essential for execution planning, function calls, and understanding operator implementation details.

## Parameters / Member Variables
- `op`: Operator tuple (HeapTuple) from pg_operator system catalog

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_operator
  - GETSTRUCT
- Called from (representative examples):
  - (Based on header reference only, actual usage locations would need further investigation)

## Notes and Other Information
- Returns the OID of the function in pg_proc that implements the operator
- Critical for linking operators to their computational implementations
- Used in query execution planning and operator evaluation
- Part of PostgreSQL's operator-to-function mapping system
- The returned function OID can be used to look up function details in pg_proc
- Essential for understanding operator behavior and performance characteristics
- Provides the bridge between operator syntax and function execution

## Simplified Source

```c
/*
 * Extract the implementing function's OID from an operator tuple.
 * Every operator in PostgreSQL is backed by a function that
 * performs the actual computation.
 */
Oid
oprfuncid(Operator op)
{
    // Get the pg_operator form from the tuple
    Form_pg_operator pgopform = (Form_pg_operator) GETSTRUCT(op);

    // Return the OID of the implementing function
    return pgopform->oprcode;
}
```