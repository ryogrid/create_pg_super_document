# pg_input_error_info

## Location
[src/backend/utils/adt/misc.c:716-764](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/misc.c#L716-L764)

## Overview
pg_input_error_info is a SQL-callable function that tests input validity for a data type and returns detailed error information if the input is invalid, or NULL if valid.

## Definition

```c
Datum
pg_input_error_info(PG_FUNCTION_ARGS)
```
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

## Simplified Source

```c
Datum pg_input_error_info(PG_FUNCTION_ARGS) {
    text *txt = PG_GETARG_TEXT_PP(0);
    text *typname = PG_GETARG_TEXT_PP(1);
    ErrorSaveContext escontext = {T_ErrorSaveContext};
    TupleDesc tupdesc;
    Datum values[4];
    bool isnull[4];

    // Validate return type
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");

    // Enable detailed error information collection
    escontext.details_wanted = true;

    // Test input validity using common validation function
    if (pg_input_is_valid_common(fcinfo, txt, typname, &escontext)) {
        // Input is valid - return all NULLs
        memset(isnull, true, sizeof(isnull));
    } else {
        // Input failed validation - extract error details
        Assert(escontext.error_occurred);
        Assert(escontext.error_data != NULL);
        Assert(escontext.error_data->message != NULL);

        memset(isnull, false, sizeof(isnull));

        // Primary error message
        values[0] = CStringGetTextDatum(escontext.error_data->message);

        // Detail message (may be NULL)
        if (escontext.error_data->detail != NULL)
            values[1] = CStringGetTextDatum(escontext.error_data->detail);
        else
            isnull[1] = true;

        // Hint message (may be NULL)
        if (escontext.error_data->hint != NULL)
            values[2] = CStringGetTextDatum(escontext.error_data->hint);
        else
            isnull[2] = true;

        // SQL error code
        char *sqlstate = unpack_sql_state(escontext.error_data->sqlerrcode);
        values[3] = CStringGetTextDatum(sqlstate);
    }

    return HeapTupleGetDatum(heap_form_tuple(tupdesc, values, isnull));
}
```