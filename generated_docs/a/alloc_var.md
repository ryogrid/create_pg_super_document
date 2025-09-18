# alloc_var

## Location
[src/backend/utils/adt/numeric.c:6969-6984](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L6969-L6984)

## Overview
Allocates a digit buffer for a NumericVar structure with specified number of digits plus one spare digit for rounding operations.

## Definition
```c
static void alloc_var(NumericVar *var, int ndigits)
```

## Detailed Description
The `alloc_var` function is a PostgreSQL memory management utility that allocates and initializes a digit buffer for NumericVar structures. It handles the allocation of memory for numeric digit storage, ensuring proper buffer management by first freeing any existing buffer, then allocating a new buffer with the requested number of digits plus one additional spare digit for rounding operations.

The function sets up the NumericVar structure's buffer pointers correctly, with `buf` pointing to the start of the allocated memory and `digits` pointing to the first usable digit (after the spare digit). The spare digit at position 0 is initialized to 0 and reserved for potential carry operations during arithmetic.

## Parameters / Member Variables
- `var`: Pointer to the NumericVar structure to allocate buffer for
- `ndigits`: Number of digits to allocate space for (actual usable digits)

## Dependencies
- Functions called/Symbols referenced:
  - digitbuf_free
  - digitbuf_alloc
- Called from (representative examples):
  - NUMERIC_CAN_BE_SHORT
  - [numeric_recv](../n/numeric_recv.md)
  - [set_var_from_str](../s/set_var_from_str.md)
  - [set_var_from_num](../s/set_var_from_num.md)
  - [numericvar_deserialize](../n/numericvar_deserialize.md)
  - [int64_to_numericvar](../i/int64_to_numericvar.md)
  - [int128_to_numericvar](../i/int128_to_numericvar.md)
  - [mul_var](../m/mul_var.md)
  - [div_var](../d/div_var.md)
  - [div_var_fast](../d/div_var_fast.md)
  - [sqrt_var](../s/sqrt_var.md)
  - [random_var](../r/random_var.md)
  - [PGTYPESnumeric_new](../P/PGTYPESnumeric_new.md)
  - [PGTYPESnumeric_from_long](../P/PGTYPESnumeric_from_long.md)
  - [PGTYPESnumeric_copy](../P/PGTYPESnumeric_copy.md)
  - PGTYPESnumeric_from_decimal

## Notes and Other Information
- Static function - only accessible within numeric.c
- Automatically frees existing buffer before allocating new one
- Allocates ndigits + 1 total digits (includes spare digit for rounding)
- Sets var->buf[0] = 0 as the spare digit for carry operations
- Updates var->digits to point to buf + 1 (after spare digit)
- Sets var->ndigits to the requested number of usable digits
- Essential for NumericVar memory management throughout numeric operations
- Uses PostgreSQL's palloc() memory allocation system via digitbuf_alloc macro
- Part of the internal numeric arithmetic infrastructure