# rule

## Location
src/timezone/zic.c: 57 - 88

## Overview
The  struct represents timezone transition rules that define when daylight saving time changes occur, encapsulating the specific timing and type information needed for timezone calculations.

## Definition


## Detailed Description
The  structure is a core component of PostgreSQL's timezone handling system, specifically used in the localtime.c module for parsing and applying timezone transition rules. It stores the parameters that define when timezone transitions (such as daylight saving time changes) occur. This structure works in conjunction with timezone parsing functions to determine the exact moments when clocks should be adjusted forward or backward.

The structure supports different types of rules (indicated by r_type) and provides flexible ways to specify transition dates, whether by absolute day, week-based calculations, or other patterns. The timing information is stored as a 32-bit integer representing the time of day when the transition occurs.

## Parameters / Member Variables
- : Enum value that specifies the type of transition rule (e.g., absolute date, last Sunday of month, etc.)
- : Day component of the rule specification, interpretation depends on r_type
- : Week component for week-based rules, used when r_type indicates week-based transitions
- : Month number (1-12) when the timezone transition occurs
- : Time of day (in seconds from midnight) when the transition takes effect

## Dependencies
- Functions called/Symbols referenced:
  - r_type (enum)
  - gmtsub
  - pg_time_t
  - pg_tm
  - increment_overflow
  - increment_overflow_time
  - leapcorr
  - timesub
  - typesequiv

- Called from (representative examples):
  - getrule
  - transtime
  - tzparse
  - rulesub
  - stringrule
  - outzone

## Notes and Other Information
- This structure is primarily used in timezone parsing and calculation routines
- The rule format follows POSIX timezone specification standards
- Multiple rule structures are typically used together to define complete timezone behavior
- The structure is used extensively in timezone compilation (zic.c) and runtime timezone calculations (localtime.c)
- Rule structures are often sorted and compared using dedicated comparison functions like rule_cmp