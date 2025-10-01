# checkStringLen

## Location
[src/backend/utils/adt/jsonb.c:276-287](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L276-L287)

## Overview
Validates that a string length does not exceed the maximum allowed size for JSONB string values, providing appropriate error reporting for oversized strings.

## Definition
```c
static bool checkStringLen(size_t len, Node *escontext)
```

## Detailed Description
checkStringLen performs a simple but critical validation check to ensure that string values being converted to JSONB format do not exceed PostgreSQL's implementation limits. The function compares the provided length against JENTRY_OFFLENMASK, which represents the maximum offset that can be stored in a JSONB entry header. If the length exceeds this limit, it reports an error through the error context mechanism with detailed information about the constraint violation.

## Parameters / Member Variables
- `len`: The length of the string to be validated
- `escontext`: Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - ereturn (error reporting with context)
  - errcode, errmsg, errdetail (error formatting functions)
  - JENTRY_OFFLENMASK (maximum length constant)

## Notes and Other Information
- Returns true if the string length is acceptable, false otherwise
- Uses the soft error handling mechanism to allow graceful error recovery
- The error provides specific details about the implementation limit to help users understand the constraint
- This check is essential for maintaining JSONB data structure integrity

## Simplified Source

```c
static bool checkStringLen(size_t len, Node *escontext) {
    // Check if string exceeds JSONB implementation limit
    if (len > JENTRY_OFFLENMASK) {
        ereturn(escontext, false,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("string too long to represent as jsonb string"),
                 errdetail("Due to an implementation restriction, jsonb strings cannot exceed %d bytes.",
                          JENTRY_OFFLENMASK)));
    }

    return true;
}
```