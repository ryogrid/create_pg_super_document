# listCasts

## Location
[src/bin/psql/describe.c:4790-4907](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L4790-L4907)

## Overview
The  function implements the  psql command for displaying type cast information in a PostgreSQL database.

## Definition

```c
bool
listCasts(const char *pattern, bool verbose)
```
## Detailed Description
This function queries the  system catalog to retrieve and display information about type casts defined in the database. Type casts define how PostgreSQL can convert values from one data type to another. The function shows the source type, target type, conversion function (if any), and whether the cast is implicit.

The function constructs a complex SQL query that joins multiple system catalogs (, , , , and optionally ) to gather comprehensive cast information. It handles different cast methods:
- Binary coercible casts (no function needed)
- Input/output function casts 
- Function-based casts

The query supports pattern matching on both source and target type names and includes optional verbose output with descriptions.

## Parameters / Member Variables
- : A SQL name pattern (with optional wildcards) to filter which casts to display based on source or target type names. If NULL, all casts are shown.
- : If true, includes cast descriptions from the  catalog in the output.

## Dependencies
- Functions called/Symbols referenced:
  - : Initializes a dynamic string buffer
  - : Adds formatted text to the buffer
  - : Appends formatted text to the buffer
  - : Validates and processes SQL name patterns for both source and target types
  - : Executes the constructed SQL query
  - : Formats and displays the query results with column translation
  - : Cleans up the string buffer
  - : Macro to get array length
- Constants used:
  - : Binary coercible cast method
  - : Input/output function cast method
  - : Explicit cast context
  - : Assignment cast context
- Called from (representative examples):
  - : Main dispatcher for psql describe commands

## Notes and Other Information
- The function returns  on success,  on failure
- Uses selective column translation for internationalization
- Pattern matching works on both source and target type names (internal and external formats)
- The 'Implicit?' column shows cast context: 'yes' (implicit), 'in assignment', or 'no' (explicit only)
- Function names like '(binary coercible)' and '(with inout)' are not localized to avoid translation conflicts
- Results are ordered by source type and target type names
- Uses error handling with goto for cleanup on validation failures