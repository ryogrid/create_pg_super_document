# FromCharDateMode

## Location
[src/backend/utils/adt/formatting.c:142-150](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L142-L150)

## Overview
An enumeration that defines different date interpretation modes used in the PostgreSQL formatting system to prevent mixing incompatible date conventions during string-to-date parsing.

## Definition

```c
typedef struct
{
	const char *name;
	int			len;
	int			id;
	bool		is_digit;
	FromCharDateMode date_mode;
} KeyWord;
```
## Detailed Description
FromCharDateMode is used by PostgreSQL's formatting system to enforce consistency in date parsing operations. When parsing date strings using format templates (like in  function), this enum ensures that format elements from different date systems (Gregorian calendar vs. ISO 8601 week dates) are not mixed within the same parsing operation.

The enum tracks the current parsing mode:
- NONE: No specific mode set yet, or for elements that don't affect date mode
- GREGORIAN: Standard calendar date elements (DD, MM, YYYY, etc.)  
- ISOWEEK: ISO 8601 week date elements (IW, IYYY, IDDD, etc.)

The system prevents mixing these modes to avoid ambiguous or contradictory date interpretations.

## Parameters / Member Variables
- : Default state indicating no date mode has been established, or used for format elements that don't influence date parsing mode (like time components, AM/PM indicators)
- : Indicates Gregorian calendar date parsing mode, used when format contains elements like DD (day), MM (month), YYYY (year), DDD (day of year)
- : Indicates ISO 8601 week date parsing mode, used when format contains elements like IW (ISO week), IYYY (ISO year), ID (ISO day), IDDD (ISO day of year)

## Dependencies
- Functions called/Symbols referenced:
  - Used in KeyWord struct as date_mode field
- Called from (representative examples):
  - from_char_set_mode
  - DCH format parsing operations
  - TmFromChar structure usage

## Notes and Other Information
- This enum is critical for data integrity during date parsing operations
- Mixing Gregorian and ISO week date conventions in a single format template will result in an error: "invalid combination of date conventions"
- Each format element in the DCH_poz array is associated with a specific FromCharDateMode value
- The mode validation occurs in the from_char_set_mode() function which prevents incompatible date convention mixing
- ISO week dates can produce different results than Gregorian dates for the same time period, making this validation essential