# jsonb_out

## Location
[src/backend/utils/adt/jsonb.c:108-123](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L108-L123)

## Overview
The  function is the output function for the JSONB data type, responsible for converting internal JSONB values back to their string representation for display or transmission.

## Definition


## Detailed Description
This function serves as the primary conversion mechanism from PostgreSQL's internal JSONB format to human-readable JSON text strings. It is automatically called by PostgreSQL's type system when JSONB values need to be displayed, returned to clients, or converted to text format. The function extracts the JSONB value from the function arguments and delegates the conversion work to , which handles the actual serialization of the JSONB structure into JSON text format.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to:
  -  (Jsonb*): The internal JSONB value to be converted to string format

## Dependencies
- Functions called/Symbols referenced:
  - : Core function that serializes JSONB structures to JSON text
  - : Macro to extract JSONB argument from function call
  - : Macro to get the size of a variable-length PostgreSQL data type
  - : Macro to return a C string from a PostgreSQL function
  - : Structure type representing internal JSONB data
- Called from (representative examples):
  - : Expression evaluation for JSON path operations
  - : Retrieval of JSON value items as strings during execution

## Notes and Other Information
- This function is registered as the output function for the JSONB type in PostgreSQL's type system
- The conversion preserves the JSON structure while ensuring proper formatting and escaping
- Uses NULL as the first parameter to , indicating default formatting behavior
- The function returns a newly allocated C string that must be managed by PostgreSQL's memory system
- Located in 
- Essential for displaying JSONB values in query results, logs, and client applications