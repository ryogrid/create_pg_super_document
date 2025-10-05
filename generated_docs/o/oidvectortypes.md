# oidvectortypes

## Location
[src/backend/utils/adt/format_type.c:447-484](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/format_type.c#L447-L484)

## Overview
A PostgreSQL function that converts a vector (array) of type OIDs into a comma-separated string list of human-readable type names.

## Definition

```c
Datum
oidvectortypes(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL system function that takes an  containing type OIDs and transforms it into a formatted text string where each OID is converted to its corresponding type name and joined with commas and spaces. This function is particularly useful for displaying function parameter types, operator argument types, and other scenarios where multiple type OIDs need to be presented in a user-friendly format.

The function implements dynamic memory management, starting with an initial buffer size estimate (20 characters per type argument) and reallocating as needed when type names exceed the available space. Each type OID is resolved using  with the  flag, which allows graceful handling of invalid or unknown type OIDs.

The output format is a comma-separated list like: "integer, text, boolean" or "numeric(10,2), varchar(50)".

## Parameters / Member Variables
- Input (via ):
  - : An  containing the array of type OIDs to be converted
- Internal variables:
  - : The dynamically allocated output string buffer
  - : Number of type OIDs in the input vector ()
  - : Current total allocated size of the result buffer
  - : Remaining space in the result buffer

## Dependencies
- Functions called/Symbols referenced:
  - : Converts individual type OIDs to formatted type names
  - : Flag allowing invalid type OIDs to be handled gracefully
  - : PostgreSQL's realloc for expanding the result buffer
  - : Converts C string to PostgreSQL text type
  - : Macro for returning text values from PostgreSQL functions
- Called from (representative examples):
  - No direct references found (likely called via SQL or internal function dispatch)

## Notes and Other Information
- This is a PostgreSQL system function accessible via SQL, likely used in system catalogs and information schema views
- Uses dynamic memory allocation with intelligent buffer sizing - starts with 20 characters per argument estimate
- Handles memory reallocation gracefully when type names exceed initial buffer estimates
- The  flag ensures the function doesn't fail on unknown or invalid type OIDs
- Returns a PostgreSQL  type that can be used in SQL queries and system functions
- The function follows PostgreSQL's fmgr (function manager) calling convention using 
- Useful for introspection of function signatures, operator definitions, and other type-related metadata

## Simplified Source

```c
Datum oidvectortypes(PG_FUNCTION_ARGS) {
    // Extract OID vector from arguments
    oidvector *oidArray = (oidvector *) PG_GETARG_POINTER(0);
    int numargs = oidArray->dim1;

    // Allocate initial buffer (20 chars per type estimate)
    size_t total = 20 * numargs + 1;
    char *result = palloc(total);
    result[0] = '\0';
    size_t left = total - 1;

    // Convert each OID to type name and build comma-separated list
    for (int num = 0; num < numargs; num++) {
        char *typename = format_type_extended(oidArray->values[num], -1,
                                            FORMAT_TYPE_ALLOW_INVALID);
        size_t slen = strlen(typename);

        // Expand buffer if needed
        if (left < (slen + 2)) {
            total += slen + 2;
            result = repalloc(result, total);
            left += slen + 2;
        }

        // Add comma separator for subsequent types
        if (num > 0) {
            strcat(result, ", ");
            left -= 2;
        }

        // Append type name
        strcat(result, typename);
        left -= slen;
    }

    PG_RETURN_TEXT_P(cstring_to_text(result));
}
```