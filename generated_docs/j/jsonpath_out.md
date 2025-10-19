# jsonpath_out

## Location
[src/backend/utils/adt/jsonpath.c:134-146](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath.c#L134-L146)

## Overview
The  function is a PostgreSQL output function for the jsonpath data type, responsible for converting the internal jsonpath representation back to its textual string format.

## Definition

```c
Datum
jsonpath_out(PG_FUNCTION_ARGS)
```
## Detailed Description
 serves as the standard output conversion function for PostgreSQL's jsonpath data type. When a jsonpath value needs to be displayed as text (such as in query results, COPY operations, or casting to text), this function is automatically called to convert the internal binary representation back to its human-readable string format. The function acts as a simple wrapper that extracts the jsonpath argument and delegates the actual conversion work to .

The function follows PostgreSQL's standard pattern for type output functions, taking a jsonpath Datum argument and returning a C-string containing the textual representation. It calculates the size of the jsonpath structure and passes it along for proper serialization.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  - : The input JsonPath structure to be converted to string format

## Dependencies
- Functions called/Symbols referenced:
  - : PostgreSQL macro for extracting jsonpath arguments
  - : Core serialization function that performs the actual jsonpath-to-string conversion
  - : PostgreSQL macro for calculating the size of variable-length data types
  - : PostgreSQL macro for returning C-string results
  - : The internal structure type representing a compiled JSON path expression
- Called from (representative examples):
  - No direct references found (typically called automatically by PostgreSQL's type system during output operations)

## Notes and Other Information
- This function is automatically invoked by PostgreSQL's type system when converting jsonpath values to text
- The actual serialization logic is implemented in , making this function a thin wrapper
- Memory allocation for the resulting string is handled through PostgreSQL's memory context system
- Part of PostgreSQL's JSON path expression support, providing the inverse operation to 
- Essential for displaying jsonpath values in query results and performing text conversions

## Simplified Source

```c
Datum jsonpath_out(PG_FUNCTION_ARGS) {
    JsonPath *in = PG_GETARG_JSONPATH_P(0);

    // Convert internal jsonpath to string representation
    PG_RETURN_CSTRING(jsonPathToCstring(NULL, in, VARSIZE(in)));
}
```

This function:
1. Extracts the jsonpath argument using PostgreSQL's argument handling macro
2. Calls the core conversion function with the jsonpath data and its size
3. Returns the resulting C-string representation using PostgreSQL's return macro