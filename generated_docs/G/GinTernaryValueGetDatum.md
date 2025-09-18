# GinTernaryValueGetDatum

## Location
[src/include/access/gin.h:75-79](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/gin.h#L75-L79)

## Overview
Converts a GinTernaryValue to a Datum type for use in PostgreSQL's function calling convention.

## Definition


## Detailed Description
This is a simple inline conversion function that casts a  to a . It serves as the counterpart to  and is part of PostgreSQL's GIN (Generalized Inverted Index) infrastructure for handling ternary logic values.

The function is used primarily in tri-consistent functions where a ternary value (TRUE, FALSE, or MAYBE) needs to be returned as a Datum, which is PostgreSQL's generic data type used for passing values between functions.

The conversion is a simple cast operation since both types have the same underlying representation -  is typedef'd as  and  can hold a char value.

## Parameters / Member Variables
- : The GinTernaryValue to convert to Datum. This can be one of the predefined constants:
  -  (0) - item is not present / does not match
  -  (1) - item is present / matches  
  -  (2) - unknown if item is present / matches

## Dependencies
- Functions called/Symbols referenced:
  - GinTernaryValue (parameter type)
- Called from (representative examples):
  - PG_RETURN_GIN_TERNARY_VALUE (macro)

## Notes and Other Information
- This function is declared as  for performance, as it's a simple cast operation that benefits from inlining
- It's defined in  at lines 74-78
- The function is part of a pair with , which performs the reverse conversion
- It's primarily used through the  macro rather than called directly
- The  type is specifically designed to be the same size as a  to allow safe pointer casting in some contexts