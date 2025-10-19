# macaddr_or

## Location
[src/backend/utils/adt/mac.c:320-340](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/mac.c#L320-L340)

## Overview
Performs bitwise OR operation between two MAC addresses, returning a new MAC address with each byte being the result of the OR operation on corresponding bytes.

## Definition

```c
Datum
macaddr_or(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements bitwise OR operation for PostgreSQL's  data type. It takes two MAC addresses as input arguments and computes the bitwise OR of each corresponding byte pair (a through f) to produce a new MAC address. This function is typically used in network address operations where you need to combine or set specific bits in MAC addresses. The function allocates memory for the result using PostgreSQL's memory management system () and returns the result using PostgreSQL's function call convention.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - First argument:  - The first MAC address operand
  - Second argument:  - The second MAC address operand

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts macaddr arguments from function call
  -  - PostgreSQL memory allocation function
  -  - Returns macaddr result following PostgreSQL conventions
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- Each byte of the MAC address (a, b, c, d, e, f) is processed independently using bitwise OR
- Memory for the result is allocated in the current memory context
- This function follows PostgreSQL's V1 calling convention for built-in functions
- Commonly used for setting specific bits or combining MAC address patterns

## Simplified Source

```c
Datum macaddr_or(PG_FUNCTION_ARGS) {
    // Extract input MAC addresses
    macaddr *addr1 = PG_GETARG_MACADDR_P(0);
    macaddr *addr2 = PG_GETARG_MACADDR_P(1);

    // Allocate memory for result
    macaddr *result = (macaddr *) palloc(sizeof(macaddr));

    // Perform bitwise OR on each byte
    result->a = addr1->a | addr2->a;
    result->b = addr1->b | addr2->b;
    result->c = addr1->c | addr2->c;
    result->d = addr1->d | addr2->d;
    result->e = addr1->e | addr2->e;
    result->f = addr1->f | addr2->f;

    PG_RETURN_MACADDR_P(result);
}
```