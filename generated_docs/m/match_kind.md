# match_kind

## Location
[src/backend/utils/activity/pgstat_shmem.c:1059-1064](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_shmem.c#L1059-L1064)

## Overview
This static helper function determines whether a statistics entry matches a specific statistics kind by comparing the entry's kind with a provided match criterion.

## Definition

```c
static bool
match_kind(PgStatShared_HashEntry *p, Datum match_data)
```
## Detailed Description
The function serves as a callback predicate for statistics entry filtering operations. It compares the  field of a statistics hash entry against a target kind value passed as a . This function is typically used with  to selectively reset statistics entries of a particular type.

The function extracts the integer kind value from the  parameter and compares it against the entry's key kind field, returning true when they match.

## Parameters / Member Variables
- `*p`: Pointer to a shared statistics hash entry () to be evaluated
- `match_data`: Datum containing the target statistics kind (as an integer) to match against
## Dependencies
- Functions called/Symbols referenced:
  - : Converts Datum to int32 value to extract the target kind
  - : Hash entry structure containing statistics metadata
- Called from (representative examples):
  - : Uses this function to match entries of a specific statistics kind

## Notes and Other Information
- This is a static (file-scope) helper function, not exposed outside pgstat_shmem.c
- Designed as a callback function matching the signature expected by 
- The  parameter must contain a valid int32 value representing a statistics kind
- Simple comparison function that enables kind-based filtering of statistics entries