# quote_literal

## Location
[src/backend/utils/adt/quote.c:78-102](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/quote.c#L78-L102)

## Overview
A PostgreSQL built-in function that takes a text input and returns a properly quoted string literal, suitable for safe inclusion in dynamically constructed SQL statements.

## Definition


## Detailed Description
The `quote_literal` function is a PostgreSQL SQL function that converts input text into a properly quoted and escaped SQL string literal. It handles PostgreSQL's text type by extracting the raw data, calculating the required buffer size (worst-case scenario of doubling all characters plus quotes and header), and then calling the internal `quote_literal_internal` function to perform the actual quoting and escaping. The function ensures that special characters like single quotes and backslashes are properly escaped, making the result safe for use in SQL queries.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to function arguments
  - Argument 0: `text` - The input text to be quoted as a SQL literal

## Dependencies
- Functions called/Symbols referenced:
  - `VARDATA` - Macro to get the data portion of a PostgreSQL variable-length type
  - [quote_literal_internal](quote_literal_internal.md) - Core function that performs the actual quoting logic
  - `SET_VARSIZE` - Macro to set the size of a variable-length PostgreSQL type
  - `PG_RETURN_TEXT_P` - Macro for returning text values from PostgreSQL functions
- Called from (representative examples):
  - [get_publications_str](../g/get_publications_str.md) - Function in subscription commands for publication name quoting
  - [quote_nullable](quote_nullable.md) - Function that handles NULL values in addition to literal quoting

## Notes and Other Information
- This function is exposed as a SQL function that can be called directly from SQL queries
- Allocates memory for worst-case scenario (all characters doubled plus quotes and header)
- Essential for preventing SQL injection when incorporating user data into dynamic queries
- Part of PostgreSQL's quote utility functions located in `src/backend/utils/adt/quote.c`
- The actual quoting logic is delegated to the `quote_literal_internal` helper function
- Used internally by PostgreSQL for subscription and publication management