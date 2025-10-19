# bitoctetlength

## Location
[src/backend/utils/adt/varbit.c:1231-1242](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varbit.c#L1231-L1242)

## Overview
Returns the length in bytes (octets) of a bit string, effectively calculating the storage space required for the variable-length bit string.

## Definition

```c
Datum
bitoctetlength(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that computes the number of bytes (octets) required to store a variable-length bit string (). This function provides the actual storage size of the bit data portion, which is useful for understanding memory usage and storage requirements. The function uses the  macro to extract the byte count from the bit string's header information.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - arg[0]:  - The input bit string whose byte length is to be calculated

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts VarBit argument from function call
  -  - Macro to get byte count from VarBit structure
  -  - Returns 32-bit integer result
- Called from (representative examples):
  - Available as SQL function 

## Notes and Other Information
- The function returns the number of bytes, not bits, so it represents the actual storage overhead
- This is different from  which returns the number of significant bits
- The return value includes any padding bytes needed for byte alignment
- Located in src/backend/utils/adt/varbit.c:1231-1242

## Simplified Source

```c
Datum bitoctetlength(PG_FUNCTION_ARGS) {
    // Extract the bit string argument
    VarBit *arg = PG_GETARG_VARBIT_P(0);

    // Return the length in bytes (storage space required)
    PG_RETURN_INT32(VARBITBYTES(arg));
}
``` 