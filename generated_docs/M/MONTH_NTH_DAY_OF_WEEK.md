# MONTH_NTH_DAY_OF_WEEK

## Location
src/timezone/localtime.c: 69 - 71

## Overview
Enumeration value representing a timezone rule type that specifies the nth occurrence of a particular day of the week within a specific month (Mm.n.d format).

## Definition

```c
struct rule
{
	enum r_type r_type;			/* type of rule */
	int			r_day;			/* day number of rule */
	int			r_week;			/* week number of rule */
	int			r_mon;			/* month number of rule */
	int32		r_time;			/* transition time of rule */
};
```
## Detailed Description
MONTH_NTH_DAY_OF_WEEK is an enumeration constant within the  enum used in PostgreSQL's timezone handling system. It represents one of three possible rule types for defining daylight saving time transitions and other timezone rules.

This specific rule type follows the "Mm.n.d" format where:
- m = month (1-12)
- n = week number within the month (1-5)
- d = day of the week (0-6, where 0 = Sunday)

For example, "M3.2.0" would represent the 2nd Sunday in March.

When this rule type is used, the system employs Zeller's Congruence algorithm to calculate the day-of-week of the first day of the specified month, then determines which specific date corresponds to the nth occurrence of the target day within that month.

## Parameters / Member Variables
This is an enumeration constant with no parameters or member variables. It is used as a value assignment to the  field in timezone rule structures.

## Dependencies
- Functions called/Symbols referenced:
  - None (enumeration constant)
  
- Called from (representative examples):
  -  at src/timezone/localtime.c:794
  -  at src/timezone/localtime.c:879

## Notes and Other Information
- Part of the POSIX timezone rule parsing and calculation system
- Used in conjunction with the  which contains fields for month (), week (), day (), and transition time ()
- The algorithm implementation uses Zeller's Congruence to determine the day-of-week for the first day of the specified month
- Handles edge cases where the nth occurrence might not exist in shorter months
- This rule type is commonly used for defining daylight saving time transitions in POSIX timezone strings