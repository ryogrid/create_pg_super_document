# NonFiniteIntervalPart

## Location
[src/backend/utils/adt/timestamp.c:5906-5950](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L5906-L5950)

## Overview
Handles extraction of time parts from infinite intervals, returning appropriate infinity values or zero for NULL results depending on the unit type.

## Definition

```c
struct pg_itm tt,
			   *tm = &tt;
```
## Detailed Description
This static function is specifically designed to handle interval part extraction when dealing with infinite intervals. It categorizes time units into two groups: oscillating units (like seconds, minutes, months) that return zero (indicating NULL should be returned), and monotonically-increasing units (like hours, days, years) that return positive or negative infinity based on the interval's sign.

The function ensures error handling consistency between finite and infinite interval cases by throwing identical errors for invalid units. This maintains uniform behavior across PostgreSQL's interval processing functions.

## Parameters / Member Variables
- : The type classification of the unit (must be UNITS or RESERV)
- : The specific time unit constant (DTK_MICROSEC, DTK_HOUR, etc.)
- : String representation of the unit name for error messages
- : Boolean indicating if the infinite interval is negative

## Dependencies
- Functions called/Symbols referenced:
  - [get_float8_infinity](../g/get_float8_infinity.md)
  - ereport (for error handling)
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [format_type_be](../f/format_type_be.md)
- Constants referenced:
  - UNITS, RESERV (type categories)
  - DTK_MICROSEC, DTK_MILLISEC, DTK_SECOND, DTK_MINUTE, DTK_MONTH, DTK_QUARTER (oscillating units)
  - DTK_HOUR, DTK_DAY, DTK_YEAR, DTK_DECADE, DTK_CENTURY, DTK_MILLENNIUM, DTK_EPOCH (monotonic units)
  - INTERVALOID (for error formatting)
- Called from:
  - [interval_part_common](../i/interval_part_common.md)

## Notes and Other Information
The function distinguishes between oscillating units (which have no meaningful infinite value and return 0 to indicate NULL) and monotonically-increasing units (which can meaningfully be infinite). This design choice reflects the mathematical properties of these time units when applied to infinite intervals. Error messages are carefully crafted to match those in calling functions to ensure consistent user experience.

## Simplified Source

```c
static float8 NonFiniteIntervalPart(int type, int unit, char *lowunits, bool isNegative) {
    // Validate unit type
    if ((type != UNITS) && (type != RESERV)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("unit \"%s\" not recognized for type %s",
                       lowunits, format_type_be(INTERVALOID))));
    }

    switch (unit) {
        // Oscillating units return 0 (NULL indicator)
        case DTK_MICROSEC:
        case DTK_MILLISEC:
        case DTK_SECOND:
        case DTK_MINUTE:
        case DTK_MONTH:
        case DTK_QUARTER:
            return 0.0;

        // Monotonic units return infinity with appropriate sign
        case DTK_HOUR:
        case DTK_DAY:
        case DTK_YEAR:
        case DTK_DECADE:
        case DTK_CENTURY:
        case DTK_MILLENNIUM:
        case DTK_EPOCH:
            return isNegative ? -get_float8_infinity() : get_float8_infinity();

        default:
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("unit \"%s\" not supported for type %s",
                           lowunits, format_type_be(INTERVALOID))));
            return 0.0;
    }
}
```