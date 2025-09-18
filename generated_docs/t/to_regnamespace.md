# to_regnamespace

## Location
src/backend/utils/adt/regproc.c: 1700 - 1717

## Overview
The to_regnamespace function converts namespace (schema) names to regnamespace OID values, returning NULL if the name is not found rather than raising an error.

## Definition


## Detailed Description
This function is a PostgreSQL conversion function that provides a safe way to convert text namespace names to regnamespace OID values. Unlike regnamespacein, which raises errors for non-existent namespaces, to_regnamespace returns NULL when the specified namespace cannot be found. This makes it suitable for use in contexts where a gentle failure mode is preferred over throwing exceptions.

The function internally uses the regnamespacein function through PostgreSQL's DirectInputFunctionCallSafe mechanism, which allows it to catch any errors that would normally be thrown and convert them to NULL returns instead. It first converts the input text argument to a C string, then sets up an error context for safe error handling, and finally calls the underlying regnamespacein function in a protected manner.

This pattern is commonly used in PostgreSQL for "safe" conversion functions that need to handle invalid input gracefully without disrupting query execution.

## Parameters / Member Variables
- The function uses PostgreSQL's standard function call interface (PG_FUNCTION_ARGS) which provides:
  - Input text argument containing the namespace name
  - : Converted C string from the input text
  - : Output Datum containing the OID or NULL
  - : Error save context for safe error handling

## Dependencies
- Functions called/Symbols referenced:
  - text_to_cstring (converts text input to C string)
  - ErrorSaveContext (error context type for safe handling)
  - regnamespacein (underlying conversion function)
  - DirectInputFunctionCallSafe (safe function call wrapper)
  - PG_RETURN_DATUM (returns the result Datum)
- Called from (representative examples):
  - This function is typically used in SQL queries where safe namespace lookups are needed

## Notes and Other Information
- Provides a non-throwing alternative to regnamespacein for namespace name lookups
- Returns NULL instead of raising errors when namespace names are not found
- Uses PostgreSQL's safe function call mechanism to catch and handle errors gracefully
- Part of PostgreSQL's family of "to_reg*" functions that provide safe conversion semantics
- Commonly used in applications where namespace existence checking is needed without error handling complexity
- Located in src/backend/utils/adt/regproc.c with other reg* type functions