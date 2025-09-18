# numeric_recv

## Location
src/backend/utils/adt/numeric.c: 1076 - 1160

## Overview
This function deserializes a PostgreSQL Numeric value from its external binary representation, converting the binary format received over the network or from storage back into the internal Numeric data type.

## Definition


## Detailed Description
The  function is the binary input function for PostgreSQL's Numeric data type. It reads a binary representation from a StringInfo buffer and reconstructs the internal Numeric value. The external binary format consists of a sequence of int16 values: ndigits (length), weight (decimal position), sign (positive/negative/special), dscale (display scale), followed by the actual numeric digits. The function performs extensive validation of the received data, including checking sign values, scale values, and individual digits. It handles both regular numeric values and special values (NaN, ±Infinity). After reconstruction, it applies any necessary truncation and typmod constraints before returning the final Numeric value.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS macro which provides:
  - : StringInfo containing the binary data to deserialize
  - : OID of the element type (unused, marked with NOT_USED)
  - : Type modifier specifying precision/scale constraints

## Dependencies
- Functions called/Symbols referenced:
  - init_var
  - [pq_getmsgint](../p/pq_getmsgint.md)
  - [alloc_var](../a/alloc_var.md)
  - NUMERIC_POS, NUMERIC_NEG, NUMERIC_NAN, NUMERIC_PINF, NUMERIC_NINF
  - NUMERIC_DSCALE_MASK
  - NBASE
  - NumericDigit
  - [trunc_var](../t/trunc_var.md)
  - [apply_typmod](../a/apply_typmod.md)
  - [apply_typmod_special](../a/apply_typmod_special.md)
  - [make_result](../m/make_result.md)
  - [free_var](../f/free_var.md)
  - PG_RETURN_NUMERIC
  - ereport, errcode, errmsg (for error handling)
- Called from:
  - Used as a PostgreSQL type input function (registered in system catalogs)

## Notes and Other Information
- This is a PostgreSQL function interface (uses PG_FUNCTION_ARGS/PG_RETURN_NUMERIC macros)
- Performs comprehensive validation of binary input data to prevent corruption
- Handles both regular numeric values and special values with appropriate processing paths
- If dscale would hide digits, they are truncated rather than causing an error for client compatibility
- Uses PostgreSQL's message protocol functions (pq_getmsgint) for binary deserialization  
- Located in src/backend/utils/adt/numeric.c:1076-1160
- Essential for binary protocol communication and storage/retrieval of Numeric values