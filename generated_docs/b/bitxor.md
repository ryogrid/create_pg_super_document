# bitxor

## Location
[src/backend/utils/adt/varbit.c:1324-1364](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varbit.c#L1324-L1364)

## Overview
Performs a logical XOR (exclusive OR) operation on two bit strings of equal length, returning a new bit string containing the bitwise XOR result.

## Definition

```c
Datum
bitxor(PG_FUNCTION_ARGS)
```
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
  - [palloc](../p/palloc.md) - Allocates memory for result
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

## Simplified Source

```c
Datum
bitxor(PG_FUNCTION_ARGS)
{
    VarBit *arg1 = PG_GETARG_VARBIT_P(0);
    VarBit *arg2 = PG_GETARG_VARBIT_P(1);
    VarBit *result;
    int bitlen1, bitlen2, i;
    bits8 *p1, *p2, *r;

    // Check that both bit strings have the same length
    bitlen1 = VARBITLEN(arg1);
    bitlen2 = VARBITLEN(arg2);
    if (bitlen1 != bitlen2)
        ereport(ERROR, "cannot XOR bit strings of different sizes");

    // Allocate result with same size as input
    result = (VarBit *) palloc(VARSIZE(arg1));
    SET_VARSIZE(result, VARSIZE(arg1));
    VARBITLEN(result) = bitlen1;

    // Perform bitwise XOR operation byte by byte
    p1 = VARBITS(arg1);
    p2 = VARBITS(arg2);
    r = VARBITS(result);
    for (i = 0; i < VARBITBYTES(arg1); i++)
        *r++ = *p1++ ^ *p2++;

    return result;
}
```