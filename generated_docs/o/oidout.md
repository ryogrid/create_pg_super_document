# oidout

## Location
[src/backend/utils/adt/oid.c:47-59](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/oid.c#L47-L59)

## Overview
The oidout function converts an internal OID (object identifier) value to its string representation, serving as the output function for the OID data type in PostgreSQL.

## Definition


## Detailed Description
The oidout function is responsible for converting PostgreSQL's internal OID representation to a human-readable string format. It allocates memory for the result string and uses snprintf to format the unsigned integer OID value as a decimal string. This function is part of PostgreSQL's type input/output system and is automatically called when converting OID values to text in SQL operations or when displaying OID values to users.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function call context and arguments
  -  (extracted via PG_GETARG_OID(0)): The OID value to be converted to string format

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md): PostgreSQL memory allocation function to allocate space for the result string
  - snprintf: Standard C library function to format the OID as a decimal string
  - PG_RETURN_CSTRING: Macro to return the formatted string result
- Called from (representative examples):
  - [plperl_trigger_build_args](../p/plperl_trigger_build_args.md): Used in PL/Perl trigger argument construction
  - [PLy_trigger_build_args](../P/PLy_trigger_build_args.md): Used in PL/Python trigger argument construction
  - [pltcl_trigger_handler](../p/pltcl_trigger_handler.md): Used in PL/Tcl trigger handling

## Notes and Other Information
- This function is registered as the output function for the OID data type in PostgreSQL's type system
- The function allocates exactly 12 bytes for the result string, which is sufficient for any 32-bit unsigned integer plus null terminator
- Memory is allocated in the current memory context and will be automatically freed when the context is reset
- The function follows PostgreSQL's V1 calling convention using the PG_FUNCTION_ARGS macro
- Location: src/backend/utils/adt/oid.c:47-59