# numeric_recv

## Location
[src/backend/utils/adt/numeric.c:1076-1160](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L1076-L1160)

## Overview
This function deserializes a PostgreSQL Numeric value from its external binary representation, converting the binary format received over the network or from storage back into the internal Numeric data type.

## Definition

```c
Datum
numeric_recv(PG_FUNCTION_ARGS)
```
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

## Simplified Source

```c
Datum numeric_recv(PG_FUNCTION_ARGS) {
    StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);
    int32 typmod = PG_GETARG_INT32(2);
    NumericVar value;
    Numeric res;
    int len, i;

    init_var(&value);

    // Read the binary format: ndigits, weight, sign, dscale, digits
    len = (uint16) pq_getmsgint(buf, sizeof(uint16));
    alloc_var(&value, len);

    value.weight = (int16) pq_getmsgint(buf, sizeof(int16));

    value.sign = (uint16) pq_getmsgint(buf, sizeof(uint16));
    // Validate sign value
    if (!(value.sign == NUMERIC_POS || value.sign == NUMERIC_NEG ||
          value.sign == NUMERIC_NAN || value.sign == NUMERIC_PINF ||
          value.sign == NUMERIC_NINF))
        ereport(ERROR, (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                errmsg("invalid sign in external \"numeric\" value")));

    value.dscale = (uint16) pq_getmsgint(buf, sizeof(uint16));
    // Validate scale value
    if ((value.dscale & NUMERIC_DSCALE_MASK) != value.dscale)
        ereport(ERROR, (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                errmsg("invalid scale in external \"numeric\" value")));

    // Read digit values
    for (i = 0; i < len; i++) {
        NumericDigit d = pq_getmsgint(buf, sizeof(NumericDigit));
        if (d < 0 || d >= NBASE)
            ereport(ERROR, (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
                    errmsg("invalid digit in external \"numeric\" value")));
        value.digits[i] = d;
    }

    // Apply truncation and typmod constraints
    if (value.sign == NUMERIC_POS || value.sign == NUMERIC_NEG) {
        trunc_var(&value, value.dscale);
        (void) apply_typmod(&value, typmod, NULL);
        res = make_result(&value);
    } else {
        // Handle special values
        res = make_result(&value);
        (void) apply_typmod_special(res, typmod, NULL);
    }

    free_var(&value);
    PG_RETURN_NUMERIC(res);
}
```