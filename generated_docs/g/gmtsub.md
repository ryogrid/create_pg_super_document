# gmtsub

## Location
[src/timezone/localtime.c:1357-1388](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/localtime.c#L1357-L1388)

## Overview
Converts a UTC timestamp to GMT representation with optional offset adjustment, serving as the GMT equivalent of the  function for local time conversion.

## Definition

```c
static struct pg_tm *
gmtsub(pg_time_t const *timep, int32 offset,
	   struct pg_tm *tmp)
```
## Detailed Description
The  function performs UTC-to-GMT time conversion with support for timezone offset adjustments. It maintains a static GMT timezone state structure that is lazily initialized on first use. The function is designed as the GMT counterpart to , providing consistent time conversion functionality for GMT/UTC operations.

Key features include:
1. **Lazy initialization**: The GMT state structure is allocated and initialized only on first use through 
2. **Offset support**: Can apply arbitrary timezone offsets to the base GMT time
3. **Timezone abbreviation handling**: Sets appropriate timezone abbreviation strings based on whether an offset is applied
4. **Error handling**: Returns NULL if memory allocation fails during initialization

The function is a critical component in PostgreSQL's timezone system, providing the foundation for GMT-based time operations and serving as a fallback for timezone conversions.

## Parameters / Member Variables
- : Pointer to a  value representing the UTC timestamp to convert to GMT representation.
- : A 32-bit signed integer representing the timezone offset in seconds to apply to the base GMT time.
- : Pointer to a  structure that will be populated with the converted GMT time values.

## Dependencies
- Functions called/Symbols referenced:
  -  (memory allocation for GMT state structure)
  -  (initializes the GMT timezone state data)
  -  (performs the actual time structure calculation)
  -  (wildcard abbreviation for non-zero offsets)
- Called from (representative examples):
  -  (fallback when timezone state is NULL, in src/timezone/localtime.c:1268)
  -  (in src/timezone/localtime.c:1391)
  - Referenced in  struct declaration (in src/timezone/localtime.c:85)

## Notes and Other Information
- This is a static function, only accessible within the localtime.c file
- Uses a static variable  to maintain the GMT state across function calls, implementing a singleton pattern for GMT timezone data
- The function includes a comment about potentially providing more sophisticated timezone abbreviations like "+xx" or "-xx" for non-zero offsets, but opts for simplicity
- When offset is zero, uses the actual GMT timezone abbreviation; when non-zero, uses a generic wildcard abbreviation
- Memory allocation failure during GMT state initialization will cause the function to return NULL
- The GMT state is initialized exactly once and reused for all subsequent calls, providing efficiency for repeated GMT conversions
- Critical for PostgreSQL's timezone infrastructure, especially for operations requiring GMT as a reference point