# JsonbToCStringIndent

## Location
[src/backend/utils/adt/jsonb.c:482-490](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L482-L490)

## Overview
A public function that converts a JSONB container to its C-string representation with pretty-printing indentation, serving as a convenience wrapper for JsonbToCStringWorker.

## Definition

```c
char *
JsonbToCStringIndent(StringInfo out, JsonbContainer *in, int estimated_len)
```
## Detailed Description
This function provides an interface for converting JSONB data to a formatted string representation with indentation and newlines for improved readability. It serves as a wrapper around JsonbToCStringWorker with indentation enabled (true). The function is primarily used for pretty-printing JSONB values where human readability is important, such as in debugging output or user-facing displays. Like its non-indented counterpart, it can work with either an existing StringInfo buffer or allocate a new string.

## Parameters / Member Variables
- `out`: Optional StringInfo buffer where the formatted result will be stored; if NULL, a new string is allocated
- `*in`: Pointer to the JsonbContainer structure containing the JSONB data to be converted
- `estimated_len`: Estimated length of the resulting string for buffer pre-allocation optimization
## Dependencies
- Functions called/Symbols referenced:
  - [JsonbToCStringWorker](JsonbToCStringWorker.md) (the core conversion function with indentation control)
  - [JsonbContainer](JsonbContainer.md) (JSONB container structure type)
- Called from (representative examples):
  - [jsonb_pretty](../j/jsonb_pretty.md) (SQL function for pretty-printing JSONB)
  - PG_RETURN_JSONB_P (macro for returning JSONB values with formatting)

## Notes and Other Information
- This is a public function (no static keyword) accessible to other modules
- Always calls JsonbToCStringWorker with indent=true for formatted output
- The indented output includes proper nesting with spaces and newlines for nested objects and arrays
- Returns a C-string that the caller is responsible for managing (if out was NULL)
- Primarily used for the jsonb_pretty() SQL function and other pretty-printing scenarios
- The estimated_len parameter becomes more important with indentation due to the additional formatting characters
- Part of PostgreSQL's JSONB API specifically for human-readable output
- The indentation format follows standard JSON pretty-printing conventions

## Simplified Source

```c
char *
JsonbToCStringIndent(StringInfo out, JsonbContainer *in, int estimated_len)
{
    // Convert JSONB to string with indentation enabled
    return JsonbToCStringWorker(out, in, estimated_len, true);
}
```