# convert_timevalue_to_scalar

## Location
[src/backend/utils/adt/selfuncs.c:4830-4895](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L4830-L4895)

## Overview
Converts various PostgreSQL timevalue data types to a scalar double value for use in statistical calculations and selectivity estimation.

## Definition

```c
static double
convert_timevalue_to_scalar(Datum value, Oid typid, bool *failure)
```
## Detailed Description
This function is a specialized converter that transforms PostgreSQL's various time-related data types into normalized double precision scalar values. It's primarily used by the query planner's selectivity estimation functions to perform arithmetic operations on time values for histogram analysis and statistical calculations. The function handles the complexity of different time representations by converting them to a common scalar format, enabling meaningful comparisons and mathematical operations across different time types.

The function supports major PostgreSQL time types including timestamps (with and without timezone), dates, intervals, and time values. For interval types, it uses an approximation method that converts months to days using an average month length calculation. The conversion preserves the relative ordering and differences between values, which is crucial for statistical analysis.

## Parameters / Member Variables
- : A Datum containing the time value to be converted
- : The PostgreSQL type OID identifying the specific time data type
- : Pointer to a boolean flag that gets set to true if the conversion fails due to unsupported type

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetTimestamp](../D/DatumGetTimestamp.md)
  - [DatumGetTimestampTz](../D/DatumGetTimestampTz.md)
  - [date2timestamp_no_overflow](../d/date2timestamp_no_overflow.md)
  - [DatumGetDateADT](../D/DatumGetDateADT.md)
  - [DatumGetIntervalP](../D/DatumGetIntervalP.md)
  - [DatumGetTimeADT](../D/DatumGetTimeADT.md)
  - [DatumGetTimeTzADTP](../D/DatumGetTimeTzADTP.md)
  - USECS_PER_DAY
  - DAYS_PER_YEAR
  - MONTHS_PER_YEAR
- Called from (representative examples):
  - [convert_to_scalar](convert_to_scalar.md)

## Notes and Other Information
- For INTERVAL types, the function uses an approximation where months are converted to days using 365.25/12.0 days per month
- The function properly handles infinite intervals by leveraging the fact that infinite intervals have all fields set to INT_MIN/INT_MAX
- For TIMETZ (time with timezone), the function converts to GMT-equivalent time by adjusting for the timezone offset
- This is a static function within selfuncs.c, indicating it's used internally for selectivity estimation calculations
- The function maintains the relative ordering of time values, which is essential for histogram-based selectivity estimation