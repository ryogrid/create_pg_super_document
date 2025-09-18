# bitnot

## Location
[src/backend/utils/adt/varbit.c:1365-1391](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varbit.c#L1365-L1391)

## Overview
Performs a logical NOT operation on a single bit string, returning a new bit string containing the bitwise complement (inverted bits).

## Definition


## Detailed Description
The `bitnot` function implements bitwise logical NOT operation on a single variable-length bit string (`VarBit`). This unary operation flips every bit in the input string: 0 becomes 1 and 1 becomes 0. Unlike the binary bitwise operations, this function works on a single input and creates a result of the same length. The function includes special padding handling to ensure that any unused bits in the final byte are properly zeroed, since the NOT operation would set them to 1.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - arg[0]: `VarBit *` - The bit string to be inverted

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_VARBIT_P` - Extracts VarBit argument from function call
  - `VARSIZE` - Gets total size of VarBit structure
  - `SET_VARSIZE` - Sets size in result VarBit structure
  - `VARBITLEN` - Gets and sets bit length
  - `VARBITS` - Gets pointer to bit data
  - `VARBITEND` - Gets pointer to end of bit data
  - `VARBIT_PAD_LAST` - Zeros padding bits in final byte
  - [palloc](../p/palloc.md) - Allocates memory for result
  - `PG_RETURN_VARBIT_P` - Returns VarBit result
- Called from (representative examples):
  - Available as SQL operator `~` for bit strings

## Notes and Other Information
- Works on a single input bit string (unary operation)
- Memory allocation for result matches the size of input string
- Bitwise NOT operation: result bit = NOT input_bit (0→1, 1→0)
- Requires special padding handling with `VARBIT_PAD_LAST` because NOT operation sets unused bits to 1
- The padding ensures that bits beyond the logical length are properly zeroed
- Useful for bit manipulation, creating bit masks, and logical inversions
- Located in `src/backend/utils/adt/varbit.c:1365-1391`