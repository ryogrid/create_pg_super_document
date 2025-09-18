# row_to_json

## Location
src/backend/utils/adt/json.c: 654 - 669

## Overview
SQL function that converts a PostgreSQL composite type (record/row) into its JSON object representation as a text string.

## Definition


## Detailed Description
The  function is a PostgreSQL SQL function that takes a composite type (record/row) as input and converts it to a JSON object representation. It serves as a wrapper function that calls the internal  function with  set to false, meaning the output JSON will be compact without line breaks for formatting.

The function extracts the composite datum from the function arguments, creates a StringInfo buffer to hold the result, delegates the actual conversion work to , and returns the resulting JSON string as a PostgreSQL text datum. The resulting JSON object will have field names as keys and field values as JSON values.

## Parameters / Member Variables
- Takes one argument through  macro:
  - Composite datum (any PostgreSQL record/row type)

## Dependencies
- Functions called/Symbols referenced:
  -  (macro to extract function argument)
  -  (creates a StringInfo buffer)
  -  (performs the actual composite-to-JSON conversion)
  -  (converts C string to PostgreSQL text)
  -  (macro to return text result)
- Called from:
  - SQL queries using the  function

## Notes and Other Information
- This function produces compact JSON output without line feeds
- For formatted JSON output with line breaks, use  instead
- The function handles all PostgreSQL composite types by delegating type-specific processing to 
- Field names become JSON object keys, and field values are converted to appropriate JSON values
- Dropped columns are automatically excluded from the JSON output
- NULL values are represented as JSON 
- Located in 