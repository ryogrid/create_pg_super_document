# rule

## Location
[src/timezone/zic.c:57-88](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L57-L88)

## Overview
The  struct represents timezone transition rules that define when daylight saving time changes occur, encapsulating the specific timing and type information needed for timezone calculations.

## Definition

```c
struct rule
{
	const char *r_filename;
	lineno_t	r_linenum;
	const char *r_name;

	zic_t		r_loyear;		/* for example, 1986 */
	zic_t		r_hiyear;		/* for example, 1986 */
	bool		r_lowasnum;
	bool		r_hiwasnum;

	int			r_month;		/* 0..11 */

	int			r_dycode;		/* see below */
	int			r_dayofmonth;
	int			r_wday;

	zic_t		r_tod;			/* time from midnight */
	bool		r_todisstd;		/* is r_tod standard time? */
	bool		r_todisut;		/* is r_tod UT? */
	bool		r_isdst;		/* is this daylight saving time? */
	zic_t		r_save;			/* offset from standard time */
	const char *r_abbrvar;		/* variable part of abbreviation */

	bool		r_todo;			/* a rule to do (used in outzone) */
	zic_t		r_temp;			/* used in outzone */
};
```
## Detailed Description
The  structure is a core component of PostgreSQL's timezone handling system, specifically used in the localtime.c module for parsing and applying timezone transition rules. It stores the parameters that define when timezone transitions (such as daylight saving time changes) occur. This structure works in conjunction with timezone parsing functions to determine the exact moments when clocks should be adjusted forward or backward.

The structure supports different types of rules (indicated by r_type) and provides flexible ways to specify transition dates, whether by absolute day, week-based calculations, or other patterns. The timing information is stored as a 32-bit integer representing the time of day when the transition occurs.

## Parameters / Member Variables
- `r_filename`: Source filename where this rule was defined
- `r_linenum`: Line number in source file where this rule was defined
- `r_name`: Name of the timezone rule
- `r_loyear`: Starting year for this rule (e.g., 1986)
- `r_hiyear`: Ending year for this rule (e.g., 1986)
- `r_lowasnum`: Whether r_loyear was specified as a number
- `r_hiwasnum`: Whether r_hiyear was specified as a number
- `r_month`: Month (0-11) when the timezone transition occurs
- `r_dycode`: Day code specifying how the transition day is determined
- `r_dayofmonth`: Day of month for the transition
- `r_wday`: Day of week for the transition
- `r_tod`: Time of day (in seconds from midnight) when the transition takes effect
- `r_todisstd`: Whether r_tod is in standard time
- `r_todisut`: Whether r_tod is in universal time
- `r_isdst`: Whether this rule represents daylight saving time
- `r_save`: Offset from standard time in seconds
- `r_abbrvar`: Variable part of timezone abbreviation
- `r_todo`: Flag indicating if this rule needs processing
- `r_temp`: Temporary storage used during processing

## Dependencies
- Functions called/Symbols referenced:
  - r_type (enum)
  - [gmtsub](../g/gmtsub.md)
  - pg_time_t
  - [pg_tm](../p/pg_tm.md)
  - [increment_overflow](../i/increment_overflow.md)
  - [increment_overflow_time](../i/increment_overflow_time.md)
  - [leapcorr](../l/leapcorr.md)
  - [timesub](../t/timesub.md)
  - [typesequiv](../t/typesequiv.md)

- Called from (representative examples):
  - [getrule](../g/getrule.md)
  - [transtime](../t/transtime.md)
  - [tzparse](../t/tzparse.md)
  - [rulesub](rulesub.md)
  - [stringrule](../s/stringrule.md)
  - [outzone](../o/outzone.md)

## Notes and Other Information
- This structure is primarily used in timezone parsing and calculation routines
- The rule format follows POSIX timezone specification standards
- Multiple rule structures are typically used together to define complete timezone behavior
- The structure is used extensively in timezone compilation (zic.c) and runtime timezone calculations (localtime.c)
- Rule structures are often sorted and compared using dedicated comparison functions like rule_cmp