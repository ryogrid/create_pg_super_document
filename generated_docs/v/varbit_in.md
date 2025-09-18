# varbit_in

## Location
src/backend/utils/adt/varbit.c: 452 - 586

## Overview
Converts a string representation of a bit string (binary or hexadecimal format) to the internal VarBit data structure for variable-length bit strings.

## Definition


## Detailed Description
The  function is a PostgreSQL built-in input function for the  (variable-length bit string) data type. It parses string representations of bit strings in either binary format (prefixed with 'b' or 'B') or hexadecimal format (prefixed with 'x' or 'X') and converts them into PostgreSQL's internal VarBit representation.

The function supports three input formats:
1. **Binary format**:  or  - each character represents one bit
2. **Hexadecimal format**:  or  - each character represents 4 bits  
3. **Implicit binary**:  - assumes binary format without prefix

Unlike the  function,  treats the type modifier () as the maximum allowed length rather than the exact required length, allowing for variable-length bit strings up to the specified limit.

## Parameters / Member Variables
-  (char*): The input string containing the bit string representation
-  (Oid): Element type OID (unused, legacy parameter)
-  (int32): Maximum allowed bit length for this varbit type, or -1 if unspecified
-  (Node*): Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING (extract input string argument)
  - PG_GETARG_INT32 (extract type modifier)
  - strlen (calculate string length)
  - VARBITMAXLEN (maximum allowed bit string length constant)
  - ereturn (soft error return with context)
  - VARBITTOTALLEN (calculate total allocation size for bit string)
  - [palloc0](../p/palloc0.md) (allocate zero-initialized memory)
  - SET_VARSIZE (set PostgreSQL variable-size header)
  - VARBITLEN (set bit string length)
  - VARBITS (get pointer to bit data)
  - HIGHBIT (high-order bit constant for parsing)
  - [pg_mblen](../p/pg_mblen.md) (multibyte character length for error reporting)
  - PG_RETURN_VARBIT_P (return VarBit result)
- Called from (representative examples):
  - PostgreSQL's type system during input conversion
  - SQL operations involving varbit literals or casts
  - COPY operations importing varbit data

## Notes and Other Information
- Supports both binary () and hexadecimal () input formats
- Validates that input characters are appropriate for the chosen format (0/1 for binary, 0-9/A-F/a-f for hex)
- Enforces maximum length constraints when type modifier is specified
- Uses soft error handling (ereturn) when error context is available for better error reporting
- Automatically zero-pads the result to ensure proper bit string format
- For hex format, validates that the total bit length doesn't exceed VARBITMAXLEN/4 characters
- Located in src/backend/utils/adt/varbit.c:452-586
- Part of PostgreSQL's comprehensive bit string type system alongside bit_in, bit_out, and related functions