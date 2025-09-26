# typesequiv

## Location
src/timezone/localtime.c: 602 - 641

## Overview
The  function determines whether two timezone type entries in a timezone state structure are equivalent by comparing all their properties.

## Definition


## Detailed Description
This static function compares two timezone type entries identified by indices  and  within a timezone state structure. It performs comprehensive equivalence checking by comparing all fields of the  structures: UTC offset, daylight saving time flag, standard time flag, UTC time flag, and the timezone designation string. The function includes bounds checking to ensure the indices are valid within the state's type array.

## Parameters / Member Variables
- : Pointer to the timezone state structure containing the type information
- : Index of the first timezone type to compare (must be within valid range)
- : Index of the second timezone type to compare (must be within valid range)

## Dependencies
- Functions called/Symbols referenced:
  - ttinfo (struct type)
  - strcmp (implicitly used for string comparison)
- Called from (representative examples):
  - tzloadbody

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