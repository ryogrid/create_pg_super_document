# typesequiv

## Location
src/timezone/localtime.c: 602 - 641

## Overview
The  function determines whether two timezone type entries in a timezone state structure are equivalent by comparing all their properties.

## Definition

```c
struct state *sp, int a, int b)
{
	bool		result;

	if (sp == NULL ||
		a < 0 || a >= sp->typecnt ||
		b < 0 || b >= sp->typecnt)
		result = false;
	else
	{
		const struct ttinfo *ap = &sp->ttis[a];
		const struct ttinfo *bp = &sp->ttis[b];

		result = (ap->tt_utoff == bp->tt_utoff
				  && ap->tt_isdst == bp->tt_isdst
				  && ap->tt_ttisstd == bp->tt_ttisstd
				  && ap->tt_ttisut == bp->tt_ttisut
				  && (strcmp(&sp->chars[ap->tt_desigidx],
							 &sp->chars[bp->tt_desigidx])
					  == 0));
	}
	return result;
}

static const int mon_lengths[2][MONSPERYEAR] = {
	{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
	{31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
};
```
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