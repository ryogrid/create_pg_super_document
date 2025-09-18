# offset_to_interval

## Location
src/backend/replication/walsender.c: 3889 - 3904

## Overview
Converts a TimeOffset value into a PostgreSQL Interval data structure for use in system views and time calculations.

## Definition
```c
static Interval *offset_to_interval(TimeOffset offset)
```

## Detailed Description
offset_to_interval is a utility function that creates a PostgreSQL Interval structure from a TimeOffset value. The function allocates memory for a new Interval structure using palloc() and initializes it with zero months and days, placing the entire time duration in the time field. This conversion is commonly used in system views where time differences need to be represented as interval types that can be displayed and manipulated using PostgreSQL's interval functions.

## Parameters / Member Variables
- `offset`: A TimeOffset value representing a time duration in microseconds

## Dependencies
- Functions called/Symbols referenced:
  - TimeOffset (type)
  - Interval (struct type)
  - [palloc](../p/palloc.md) (memory allocation function)
- Called from (representative examples):
  - PG_STAT_GET_WAL_SENDERS_COLS (multiple calls)

## Notes and Other Information
- This is a static function, only accessible within the walsender.c file
- Uses palloc() for memory allocation, which is PostgreSQL's memory management system
- The resulting Interval has zero months and days, with all time stored in the time field
- Primarily used for converting time differences into interval format for the pg_stat_replication system view
- The TimeOffset type represents time in microseconds
- Located in src/backend/replication/walsender.c at lines 3889-3904