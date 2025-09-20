# bit_and

## Location
[src/backend/utils/adt/varbit.c:1243-1283](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varbit.c#L1243-L1283)

## Overview
Performs a logical AND operation on two bit strings of equal length, returning a new bit string containing the bitwise AND result.

## Definition

```c
Datum
bit_and(PG_FUNCTION_ARGS)
```
## Detailed Description
The `bit_and` function implements bitwise logical AND operation between two variable-length bit strings (`VarBit`). The function requires both input bit strings to have exactly the same length, otherwise it raises an error. The operation is performed byte-by-byte on the underlying bit data, where each bit in the result is set to 1 only if the corresponding bits in both input strings are 1. The result maintains the same bit length as the input strings.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - arg[0]: `VarBit *` - First bit string operand
  - arg[1]: `VarBit *` - Second bit string operand

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_VARBIT_P` - Extracts VarBit arguments from function call
  - `VARBITLEN` - Gets bit length from VarBit structure
  - `VARSIZE` - Gets total size of VarBit structure
  - `SET_VARSIZE` - Sets size in result VarBit structure
  - `VARBITS` - Gets pointer to bit data
  - `VARBITBYTES` - Gets byte count for iteration
  - [palloc](../p/palloc.md) - Allocates memory for result
  - `ereport`, `errcode`, `errmsg` - Error reporting functions
  - `PG_RETURN_VARBIT_P` - Returns VarBit result
- Called from (representative examples):
  - Available as SQL operator `&` for bit strings

## Notes and Other Information
- Both input bit strings must have identical bit lengths, enforced at runtime
- Memory allocation for result matches the size of input strings
- Bitwise AND operation: result bit = bit1 AND bit2
- No padding adjustment needed since AND of 0-bits remains 0
- Error message: "cannot AND bit strings of different sizes"
- Located in `src/backend/utils/adt/varbit.c:1243-1283`