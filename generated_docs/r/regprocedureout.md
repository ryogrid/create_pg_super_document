# regprocedureout

## Location
src/backend/utils/adt/regproc.c: 435 - 451

## Overview
Converts a procedure OID to its string representation in the format "procedure_name(argument_types)" for the regprocedure data type output function.

## Definition
```c
Datum regprocedureout(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the output function for PostgreSQL's regprocedure data type. It takes a procedure OID as input and converts it to a human-readable string representation that includes the procedure name and its argument types in parentheses. The function handles the special case of InvalidOid by returning a dash ("-") character, which is the standard representation for invalid or null OIDs in PostgreSQL's reg* types.

The function is part of PostgreSQL's type system infrastructure and is automatically called whenever a regprocedure value needs to be converted to text for display or output purposes.

## Parameters / Member Variables
- Function follows PostgreSQL's standard function calling convention using PG_FUNCTION_ARGS
- Input: procedure OID (retrieved via PG_GETARG_OID(0))
- Returns: C-string representation of the procedure

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID
  - InvalidOid
  - pstrdup
  - format_procedure
  - PG_RETURN_CSTRING
  - RegProcedure (type)
- Called from:
  - Automatically invoked by PostgreSQL's type system when converting regprocedure values to text

## Notes and Other Information
- This is a standard PostgreSQL type output function that follows the naming convention of [typename]out
- Returns "-" for InvalidOid, consistent with other reg* type output functions
- The actual formatting work is delegated to format_procedure() which handles the complex logic of procedure name resolution and argument type formatting
- Used internally by PostgreSQL whenever regprocedure values need to be displayed as text (e.g., in SELECT queries, pg_dump output, etc.)