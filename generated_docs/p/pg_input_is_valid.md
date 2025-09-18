# pg_input_is_valid

## Location
[src/backend/utils/adt/misc.c:696-715](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/misc.c#L696-L715)

## Overview
pg_input_is_valid is a SQL-callable function that tests whether a given string is valid input for a specified data type, returning true if valid or false if invalid.

## Definition


## Detailed Description
This function provides a safe way to validate input strings against PostgreSQL data types without throwing errors. It takes a text string and a type name, then attempts to parse the string using the specified type's input function. Instead of raising an error on invalid input, it uses PostgreSQL's "soft error" mechanism (errsave/ereturn) to capture parsing failures and return them as boolean results.

The function relies on the underlying data type's input function being updated to support soft error reporting. For data types that haven't been updated with this mechanism, the function may still throw errors rather than returning false.

This is particularly useful for data validation scenarios where you need to check input validity before attempting actual conversion, such as in data loading or user input validation contexts.

## Parameters / Member Variables
-  (text*): The input string to validate
-  (text*): The name of the data type to validate against

## Dependencies
- Functions called/Symbols referenced:
  -  (to extract text arguments efficiently)
  -  (shared validation logic)
  -  (to return boolean result)
  -  (structure for capturing soft errors)
  -  (node tag for ErrorSaveContext)
- Called from:
  - No direct callers found in the codebase (SQL-callable function)

## Notes and Other Information
- Located in src/backend/utils/adt/misc.c:696-715
- This function is part of PostgreSQL's SQL API for input validation
- Only works reliably with data types whose input functions support soft error reporting
- Uses ErrorSaveContext to capture parsing errors without throwing exceptions
- The actual validation logic is implemented in the shared  function
- Particularly useful for ETL processes and applications that need to pre-validate data
- Returns false for invalid input rather than throwing errors, making it safe for batch validation operations