# lookup

## Location
[src/timezone/zic.c:303-389](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L303-L389)

## Overview
The  struct is a simple key-value mapping structure used in PostgreSQL's timezone compiler (zic) for translating textual keywords into their corresponding integer constants.

## Definition

```c
struct lookup
{
	const char *l_word;
	const int	l_value;
};
```
## Detailed Description
The  structure serves as a fundamental building block for keyword-to-value translation in PostgreSQL's timezone compilation system. It is used extensively throughout the zic.c module to create lookup tables that map textual representations (such as month names, day names, rule types, etc.) to their corresponding numeric values. This structure enables the timezone compiler to parse human-readable timezone definition files and convert them into the internal numeric representations needed for timezone calculations.

The structure is typically used in arrays to create lookup tables for various categories of timezone-related keywords, including months of the year, days of the week, rule types, and special year indicators. The design follows a simple but effective pattern where each entry contains a string keyword and its associated integer value.

## Parameters / Member Variables
- `*l_word`: Constant character pointer to the textual keyword or name being mapped
- `l_value`: Constant integer value that corresponds to the textual keyword

## Dependencies
- Functions called/Symbols referenced:
  - [word](../w/word.md)
  - byword
  - LC_RULE, LC_ZONE, LC_LINK, LC_LEAP, LC_EXPIRES (rule type constants)
  - TM_JANUARY through TM_DECEMBER (month constants)
  - TM_SUNDAY through TM_SATURDAY (day constants)
  - YR_MINIMUM, YR_MAXIMUM, YR_ONLY (year type constants)
  - MONSPERYEAR, DAYSPERNYER, DAYSPERLYER (calendar constants)

- Called from (representative examples):
  - byword (lookup function)
  - [infile](../i/infile.md) (file parsing)
  - getleapdatetime
  - inleap
  - rulesub
  - ciprefix

## Notes and Other Information
- Used to create static lookup tables for timezone rule parsing
- Enables conversion between human-readable timezone specifications and internal numeric codes
- Multiple lookup arrays are defined for different categories (months, days, rule types, etc.)
- The structure supports efficient keyword matching through the byword function
- Essential for parsing POSIX timezone specification files
- Also used in text search functionality (ts_selfuncs.c) and MAC address processing (mac8.c)
- The lookup tables are typically terminated with NULL entries to indicate the end of the array