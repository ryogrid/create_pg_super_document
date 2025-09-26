# PrintManyTest

## Location
[src/test/modules/test_resowner/test_resowner_many.c:83-96](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_resowner/test_resowner_many.c#L83-L96)

## Overview
A ResourceOwner callback function that provides debug information about leaked ManyTestResource objects, generating descriptive strings for diagnostic purposes.

## Definition

```c
static char *
PrintManyTest(Datum res)
```
## Detailed Description
PrintManyTest serves as the debug print callback function for the PostgreSQL ResourceOwner system when dealing with ManyTestResource objects. This function is automatically invoked by the resource management system when it needs to generate human-readable information about resources that have been leaked (not properly released).

The function performs two main operations:
1. Increments the leak counter for the specific resource kind to maintain statistics about leaked resources
2. Returns a formatted string containing descriptive information about the leaked resource

The function assumes it is called exactly once per leaked resource and that there are no other callers, as indicated by the XXX comment. This assumption allows it to accurately track leak statistics by simply incrementing the counter each time it's called.

## Parameters / Member Variables
- : A Datum containing a pointer to the ManyTestResource object to be described

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetPointer (macro to extract pointer from Datum)
  - psprintf (PostgreSQL's sprintf variant that allocates memory)
- Called from (representative examples):
  - ManyTestResource (referenced as callback)
  - InitManyTestResourceKind (registered as callback)

## Notes and Other Information
- This is a static function, only accessible within the test_resowner_many.c file
- Returns a dynamically allocated string that must be freed by the caller
- The function increments leak statistics as a side effect of being called
- Used primarily for debugging and testing resource management functionality
- The XXX comment indicates a design assumption that should be maintained
- Memory for the returned string is allocated using psprintf, following PostgreSQL memory management conventions