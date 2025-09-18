# to_regoper

## Location
[src/backend/utils/adt/regproc.c:527-544](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L527-L544)

## Overview
Converts an operator name string to an operator OID, returning NULL if the operator is not found.

## Definition


## Detailed Description
The  function is a PostgreSQL built-in function that safely converts an operator name string to its corresponding operator OID (Object Identifier). Unlike , this function does not raise an error when the operator name is not found; instead, it returns NULL. This makes it suitable for use in queries where you want to handle missing operators gracefully.

The function uses PostgreSQL's error-safe input mechanism () with an  to catch any errors that would normally be thrown during the conversion process. If the conversion fails for any reason (operator not found, invalid syntax, etc.), the function returns NULL instead of throwing an error.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function arguments
  - Argument 0: Text input containing the operator name to convert

## Dependencies
- Functions called/Symbols referenced:
  - : Converts PostgreSQL text type to C string
  - : Error handling context structure
  - : Internal operator input function for parsing operator names
  - : Safe wrapper for input functions that catches errors
  - : Macro to return a Datum value from the function

- Called from (representative examples):
  - No direct references found (typically called via SQL interface)

## Notes and Other Information
- This function is part of PostgreSQL's regtype family of functions for type/object name resolution
- The function is designed to be error-safe, making it useful in contexts where NULL handling is preferred over error throwing
- It internally uses  for the actual parsing and validation logic
- The function is typically exposed to SQL users and can be called directly in queries