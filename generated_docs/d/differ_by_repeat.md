# differ_by_repeat

## Location
src/timezone/localtime.c: 170 - 177

## Overview
Determines whether two timestamps differ by exactly one repeat cycle (approximately 400 years) in timezone calculations.

## Definition

```c
union input_buffer
{
	/* The first part of the buffer, interpreted as a header.  */
	struct tzhead tzhead;

	/* The entire buffer.  */
	char		buf[2 * sizeof(struct tzhead) + 2 * sizeof(struct state)
					+ 4 * TZ_MAX_TIMES];
};
```
## Detailed Description
The `differ_by_repeat` function checks if two timestamp values differ by exactly `SECSPERREPEAT` seconds, which represents one repeat cycle in timezone calculations. A repeat cycle is approximately 400 years (146097 days), chosen because the Gregorian calendar repeats its pattern every 400 years.

The function first performs a safety check to ensure the time type has sufficient precision to represent the repeat interval. If the time type lacks sufficient bits, the function returns false (0) to avoid potential overflow issues.

This function is used in timezone processing to detect when timezone transitions follow a repeating pattern, allowing for more efficient storage and computation of timezone rules over long time periods.

## Parameters / Member Variables
- `t1`: The later timestamp
- `t0`: The earlier timestamp

## Dependencies
- Functions called/Symbols referenced:
  - pg_time_t (timestamp type)
  - TYPE_BIT (macro for determining type bit width)
  - TYPE_SIGNED (macro for determining if type is signed)
  - SECSPERREPEAT_BITS (constant: 34, ceiling of log2(SECSPERREPEAT))
  - SECSPERREPEAT (constant: approximately 400 years worth of seconds)
- Called from (representative examples):
  - tzloadbody (calls at lines 500, 508)

## Notes and Other Information
- This is a static function, only accessible within the localtime.c compilation unit
- SECSPERREPEAT represents (YEARSPERREPEAT × AVGSECSPERYEAR) where YEARSPERREPEAT is 400 and AVGSECSPERYEAR is 31556952 seconds
- The bit precision check prevents overflow on systems with limited time_t precision
- Used to optimize timezone data storage by identifying repeating patterns in timezone transitions
- The 400-year cycle matches the Gregorian calendar's leap year pattern repetition
- Returns false immediately if the time type cannot safely represent the repeat interval