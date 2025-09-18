# bitxor

## Location
src/backend/utils/adt/varbit.c: 1324 - 1364

## Overview
Performs a logical XOR (exclusive OR) operation on two bit strings of equal length, returning a new bit string containing the bitwise XOR result.

## Definition


## Detailed Description
The `bitxor` function implements bitwise logical XOR (exclusive OR) operation between two variable-length bit strings (`VarBit`). Like the other bitwise operations, this function requires both input bit strings to have exactly the same length. The XOR operation sets each bit in the result to 1 if exactly one of the corresponding bits in the input strings is 1 (but not both). If both bits are the same (both 0 or both 1), the result bit is 0.

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
  - `[palloc](../p/palloc.md)` - Allocates memory for result
  - `ereport`, `errcode`, `errmsg` - Error reporting functions
  - `PG_RETURN_VARBIT_P` - Returns VarBit result
- Called from (representative examples):
  - Available as SQL operator `#` for bit strings

## Notes and Other Information
- Both input bit strings must have identical bit lengths, enforced at runtime
- Memory allocation for result matches the size of input strings
- Bitwise XOR operation: result bit = bit1 XOR bit2 (1 if different, 0 if same)
- No padding adjustment needed since XOR of 0-bits remains 0
- Error message: "cannot XOR bit strings of different sizes"
- XOR is useful for bit manipulation, encryption, and finding differences between bit patterns
- Located in `src/backend/utils/adt/varbit.c:1324-1364`