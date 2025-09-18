# date_cmp_timestamptz_internal

## Location
src/backend/utils/adt/date.c: 823 - 843

## Overview
Internal helper function that performs three-way comparison between a date value and a timestamptz (timestamp with timezone) value, with proper handling of overflow conditions and special timestamp values.

## Definition


## Detailed Description
This internal function implements the core comparison logic for DATE vs TIMESTAMPTZ operations in PostgreSQL. It converts the date to a timestamptz using date2timestamptz_opt_overflow() which can detect overflow conditions. The function handles three scenarios: normal conversion where it delegates to timestamptz_cmp_internal(), positive overflow where the date exceeds finite timestamp bounds, and negative overflow where the date is below finite timestamp bounds. Special timestamp values like infinity and negative infinity are properly handled during overflow conditions.

## Parameters / Member Variables
- : DateADT value representing the date to compare
- : TimestampTz value representing the timestamp with timezone to compare against

## Dependencies
- Functions called/Symbols referenced:
  - [date2timestamptz_opt_overflow](date2timestamptz_opt_overflow.md) (converts date to timestamptz with overflow detection)
  - TIMESTAMP_IS_NOEND (macro to check for positive infinity timestamp)  
  - TIMESTAMP_IS_NOBEGIN (macro to check for negative infinity timestamp)
  - timestamptz_cmp_internal (internal timestamptz comparison function)
- Data types used:
  - DateADT (PostgreSQL date type)
  - TimestampTz (PostgreSQL timestamp with timezone type)
- Called from (representative examples):
  - [date_eq_timestamptz](date_eq_timestamptz.md), date_ne_timestamptz, date_lt_timestamptz
  - [date_gt_timestamptz](date_gt_timestamptz.md), date_le_timestamptz, date_ge_timestamptz
  - [date_cmp_timestamptz](date_cmp_timestamptz.md)
  - [timestamptz_eq_date](../t/timestamptz_eq_date.md), timestamptz_ne_date, etc.
  - [cmpDateToTimestampTz](../c/cmpDateToTimestampTz.md) (in jsonpath execution)

## Notes and Other Information
- Located in src/backend/utils/adt/date.c:823-843
- This is a core internal function used by all date/timestamptz comparison operators
- Handles edge cases where date conversion to timestamptz would overflow
- Returns standard comparison result: negative, zero, or positive integer
- Properly handles PostgreSQL's special timestamp values (infinity, -infinity)
- The overflow handling ensures correct comparison semantics even at the boundaries of the timestamp range