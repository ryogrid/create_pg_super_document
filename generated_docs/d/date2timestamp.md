# date2timestamp

## Location
src/backend/utils/adt/date.c: 608 - 623

## Overview
A wrapper function that converts a DateADT value to a Timestamp, throwing an error if the conversion would result in overflow.

## Definition


## Detailed Description
The  function provides a simplified interface for date-to-timestamp conversion by calling  with a NULL overflow parameter. This forces the underlying function to throw an error rather than return an overflow indicator when the date value is outside the valid timestamp range. The function is declared static, indicating it's used internally within the date.c module for operations that require strict range validation.

## Parameters / Member Variables
- : The DateADT input value to be converted to timestamp

## Dependencies
- Functions called/Symbols referenced:
  - date2timestamp_opt_overflow: Core conversion function with overflow detection
  - DateADT: PostgreSQL internal date type
- Called from (representative examples):
  - in_range_date_interval: Date range checking with intervals
  - date_pl_interval: Date plus interval arithmetic
  - date_mi_interval: Date minus interval arithmetic  
  - date_timestamp: SQL function for date to timestamp conversion
  - datetime_timestamp: DateTime to timestamp conversion

## Notes and Other Information
- Static function scope limits usage to date.c module internal operations
- Provides error-throwing behavior for strict type conversion scenarios
- Return type is declared as TimestampTz but actually returns Timestamp (likely a documentation inconsistency)
- Serves as the standard interface for date-to-timestamp conversion when overflow should be treated as an error condition
- Essential for interval arithmetic operations that require timestamp precision