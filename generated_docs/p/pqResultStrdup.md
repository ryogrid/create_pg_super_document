# pqResultStrdup

## Location
[src/interfaces/libpq/fe-exec.c:675-691](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L675-L691)

## Overview
pqResultStrdup duplicates a string using PGresult subsidiary storage, similar to strdup but with memory allocated within the result object.

## Definition
```c
char *pqResultStrdup(PGresult *res, const char *str)
```

## Detailed Description
pqResultStrdup provides string duplication functionality that allocates the new string within PGresult subsidiary storage rather than using system malloc. This ensures that the duplicated string will be automatically freed when the PGresult is destroyed, preventing memory leaks. The function calculates the required space (string length + 1 for null terminator), allocates it using pqResultAlloc with text alignment, and copies the source string to the new location.

## Parameters / Member Variables
- `res`: Pointer to the PGresult structure where the string should be allocated
- `str`: Source string to duplicate

## Dependencies
- Functions called/Symbols referenced:
  - [pqResultAlloc](pqResultAlloc.md)
  - strlen
  - strcpy
- Called from (representative examples):
  - [PQsetResultAttrs](../P/PQsetResultAttrs.md)
  - [pqSetResultError](pqSetResultError.md)
  - [getRowDescriptions](../g/getRowDescriptions.md)
  - [pqGetErrorNotice3](pqGetErrorNotice3.md)

## Notes and Other Information
- Uses text allocation (isBinary=false) since strings don't require binary alignment
- Returns NULL if pqResultAlloc fails due to out of memory conditions
- Automatically includes space for null terminator in allocation
- Commonly used for storing field names, error messages, and other string data in results
- Located at src/interfaces/libpq/fe-exec.c:675-691

## Simplified Source

```c
char *pqResultStrdup(PGresult *res, const char *str)
{
    // Allocate space within PGresult memory (string length + null terminator)
    char *space = (char *) pqResultAlloc(res, strlen(str) + 1, false);

    // Copy string if allocation succeeded
    if (space)
        strcpy(space, str);

    return space;  // Returns NULL if allocation failed
}
```