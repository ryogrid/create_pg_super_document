# typesequiv

## Location
[src/timezone/localtime.c:602-641](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/localtime.c#L602-L641)

## Overview
The  function determines whether two timezone type entries in a timezone state structure are equivalent by comparing all their properties.

## Definition

```c
static bool typesequiv(const struct state *sp, int a, int b)
```
## Detailed Description
This static function compares two timezone type entries identified by indices  and  within a timezone state structure. It performs comprehensive equivalence checking by comparing all fields of the  structures: UTC offset, daylight saving time flag, standard time flag, UTC time flag, and the timezone designation string. The function includes bounds checking to ensure the indices are valid within the state's type array.

## Parameters / Member Variables
- `sp`: Pointer to the timezone state structure containing the type information
- `a`: Index of the first timezone type to compare (must be within valid range)
- `b`: Index of the second timezone type to compare (must be within valid range)

## Dependencies
- Functions called/Symbols referenced:
  - [ttinfo](ttinfo.md) (struct type)
  - strcmp (implicitly used for string comparison)
- Called from (representative examples):
  - [tzloadbody](tzloadbody.md)

## Notes and Other Information
- Returns  if the state pointer is NULL or if either index is out of bounds
- Returns  only if all timezone type properties are identical:
  - : UTC offset in seconds
  - : Daylight saving time flag
  - : Standard time flag  
  - : UTC time flag
  - Timezone designation strings must match exactly
- Used internally during timezone data loading to optimize storage by identifying duplicate type definitions
- Part of PostgreSQL's timezone handling optimization system