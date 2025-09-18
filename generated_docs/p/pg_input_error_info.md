# pg_input_error_info

## Location
src/backend/utils/adt/misc.c: 716 - 764

## Overview
pg_input_error_info is a SQL-callable function that tests input validity for a data type and returns detailed error information if the input is invalid, or NULL if valid.

## Definition


## Detailed Description
This function provides comprehensive error reporting for data type input validation. Unlike  which only returns a boolean, this function captures and returns the complete error information when input parsing fails, including the primary error message, detail message, hint message, and SQL error code.

The function uses PostgreSQL's "soft error" mechanism (errsave/ereturn) to capture parsing failures without throwing exceptions. It returns a composite type (row) containing four fields: message, detail, hint, and sqlstate. When the input is valid, it returns a row with all NULL values.

The function enables detailed error reporting by setting  in the ErrorSaveContext, ensuring that comprehensive error information is captured during the validation process.

## Parameters / Member Variables
-  (text*): The input string to validate
-  (text*): The name of the data type to validate against

## Return Value
Returns a composite type with four fields:
-  (text): Primary error message
-  (text): Detailed error information (may be NULL)
-  (text): Hint for resolving the error (may be NULL)  
-  (text): SQL error code

## Dependencies
- Functions called/Symbols referenced:
  -  (to extract text arguments efficiently)
  -  (to validate return type structure)
  -  (shared validation logic)
  -  (structure for capturing soft errors)
  -  (node tag for ErrorSaveContext)
  -  (constant for composite return types)
  -  (to convert C strings to PostgreSQL text)
  -  (to convert error codes to SQL state strings)
  -  (to construct the result tuple)
  -  (to return the tuple as a Datum)
  -  (for internal error reporting)
- Called from:
  - No direct callers found in the codebase (SQL-callable function)

## Notes and Other Information
- Located in src/backend/utils/adt/misc.c:716-764
- This function is part of PostgreSQL's SQL API for comprehensive input validation
- Only works reliably with data types whose input functions support soft error reporting
- Returns NULL values in all fields when input is valid
- Provides much more detailed error information than 
- The function validates that it's being called in a context expecting a composite return type
- Uses assertions to ensure error data consistency when validation fails
- Particularly useful for applications that need detailed error reporting for data validation failures