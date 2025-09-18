# array_to_json

## Location
[src/backend/utils/adt/json.c:621-636](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L621-L636)

## Overview
SQL function that converts a PostgreSQL array into its JSON representation as a text string.

## Definition


## Detailed Description
The  function is a PostgreSQL SQL function that takes an array as input and converts it to a JSON array representation. It serves as a wrapper function that calls the internal  function with  set to false, meaning the output JSON will be compact without line breaks for formatting.

The function extracts the array datum from the function arguments, creates a StringInfo buffer to hold the result, delegates the actual conversion work to , and returns the resulting JSON string as a PostgreSQL text datum.

## Parameters / Member Variables
- Takes one argument through  macro:
  - Array datum (any PostgreSQL array type)

## Dependencies
- Functions called/Symbols referenced:
  -  (macro to extract function argument)
  -  (creates a StringInfo buffer)
  -  (performs the actual array-to-JSON conversion)
  -  (converts C string to PostgreSQL text)
  -  (macro to return text result)
- Called from:
  - SQL queries using the  function

## Notes and Other Information
- This function produces compact JSON output without line feeds
- For formatted JSON output with line breaks, use  instead
- The function handles all PostgreSQL array types by delegating type-specific processing to 
- Empty arrays are represented as  in the JSON output
- Located in 