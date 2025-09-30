# DecodeInterval

## Location
[src/interfaces/ecpg/pgtypeslib/interval.c:326-679](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/interval.c#L326-L679)

## Overview
Interprets previously parsed fields for general time interval processing, converting tokenized interval string components into PostgreSQL's internal interval representation.

## Definition
```c
int DecodeInterval(char **field, int *ftype, int nf, int range, int *dtype, struct pg_itm_in *itm_in)
```

## Detailed Description
This function processes tokenized interval string components and converts them into PostgreSQL's internal interval time structure. It handles various interval formats including PostgreSQL-style intervals, SQL standard intervals, and supports special interval values like infinity.

The function implements sophisticated sign handling logic depending on the IntervalStyle setting. In SQL_STANDARD mode, a leading negative sign applies to all fields unless other explicit signs are present. It processes fields from right to left to handle units that precede values.

Key capabilities include:
- Support for all time units from microseconds to millennia
- Handling of fractional values and year-month syntax (e.g., "2-6" for 2 years 6 months)
- Processing of time notation (HH:MM:SS) with proper sign handling
- Special handling for "ago" keyword to negate the entire interval
- Support for infinite interval values (DTK_EARLY, DTK_LATE)
- Comprehensive overflow checking and error handling

## Parameters / Member Variables
- `field`: Array of string tokens representing interval components
- `ftype`: Array of field types corresponding to each token (DTK_TIME, DTK_NUMBER, DTK_STRING, etc.)
- `nf`: Number of fields in the arrays
- `range`: Interval typmod constraining which fields are allowed
- `dtype`: Output parameter indicating the result type (DTK_DELTA for normal intervals, or special values for infinity)
- `itm_in`: Output parameter containing the parsed interval components

## Dependencies
- Functions called/Symbols referenced:
  - [ClearPgItmIn](../C/ClearPgItmIn.md) - initializes the interval structure
  - [DecodeTimeForInterval](DecodeTimeForInterval.md) - parses time notation (HH:MM:SS)
  - strtoi64, strtoint - string to integer conversion
  - [ParseFraction](../P/ParseFraction.md) - parses fractional components
  - [AdjustYears](../A/AdjustYears.md), AdjustMonths, AdjustDays, AdjustMicroseconds - adjust interval components
  - [AdjustFractYears](../A/AdjustFractYears.md), AdjustFractDays, AdjustFractMicroseconds - adjust fractional components
  - [DecodeUnits](DecodeUnits.md), DecodeSpecial - decode unit names and special keywords
  - [pg_mul_s64_overflow](../p/pg_mul_s64_overflow.md), pg_add_s64_overflow - overflow-safe arithmetic
  - Constants: DTK_DELTA, DTK_TIME, DTK_NUMBER, DTK_STRING, DTK_SPECIAL, USECS_PER_SEC, USECS_PER_MINUTE, USECS_PER_HOUR, USECS_PER_DAY, DAYS_PER_MONTH, MONTHS_PER_YEAR
- Called from:
  - [interval_in](../i/interval_in.md) (src/backend/utils/adt/timestamp.c:933)
  - [PGTYPESinterval_from_asc](../P/PGTYPESinterval_from_asc.md) (src/interfaces/ecpg/pgtypeslib/interval.c:1032)

## Notes and Other Information
- Returns 0 on success, or a DTERR error code for invalid input
- Processes fields from right to left to handle unit specifications that precede their values
- Supports both PostgreSQL traditional and SQL standard sign handling modes
- The "ago" keyword must appear at the end and negates the entire interval
- Special reserved words "infinity" and "-infinity" result in infinite interval values
- Fractional parts are supported for all time units, not just the least significant
- Comprehensive validation prevents field duplication and ensures proper unit-value pairing

## Simplified Source

```c
int DecodeInterval(char **field, int *ftype, int nf, int range, int *dtype, struct pg_itm_in *itm_in)
{
    bool force_negative = false;
    bool is_before = false;
    bool parsing_unit_val = false;
    int fmask = 0, type = IGNORE_DTF;

    *dtype = DTK_DELTA;
    ClearPgItmIn(itm_in);

    // Handle SQL standard negative sign propagation
    if (IntervalStyle == INTSTYLE_SQL_STANDARD && nf > 0 && *field[0] == '-') {
        force_negative = true;
        // Check for additional explicit signs that would override global negative
        for (int i = 1; i < nf; i++) {
            if (*field[i] == '-' || *field[i] == '+') {
                force_negative = false;
                break;
            }
        }
    }

    // Process fields from right to left to handle units before values
    for (int i = nf - 1; i >= 0; i--) {
        int64 val;
        double fval;
        int tmask = 0;

        switch (ftype[i]) {
            case DTK_TIME:
                // Parse HH:MM:SS format
                if (DecodeTimeForInterval(field[i], fmask, range, &tmask, itm_in))
                    return DTERR_FIELD_OVERFLOW;
                if (force_negative && itm_in->tm_usec > 0)
                    itm_in->tm_usec = -itm_in->tm_usec;
                type = DTK_DAY;
                break;

            case DTK_NUMBER:
            case DTK_DATE:
                // Parse numeric values with potential fractions
                val = strtoi64(field[i], &cp, 10);
                if (errno == ERANGE) return DTERR_FIELD_OVERFLOW;

                // Handle year-month format (e.g., "2-6")
                if (*cp == '-') {
                    int val2 = strtoint(cp + 1, &cp, 10);
                    if (errno == ERANGE || val2 < 0 || val2 >= MONTHS_PER_YEAR)
                        return DTERR_FIELD_OVERFLOW;
                    type = DTK_MONTH;
                    val = val * MONTHS_PER_YEAR + val2;
                    fval = 0;
                } else if (*cp == '.') {
                    ParseFraction(cp, &fval);
                } else if (*cp == '\0') {
                    fval = 0;
                } else {
                    return DTERR_BAD_FORMAT;
                }

                // Apply global negative sign if needed
                if (force_negative) {
                    if (val > 0) val = -val;
                    if (fval > 0) fval = -fval;
                }

                // Adjust interval components based on unit type
                switch (type) {
                    case DTK_YEAR:
                        if (!AdjustYears(val, 1, itm_in) || !AdjustFractYears(fval, 1, itm_in))
                            return DTERR_FIELD_OVERFLOW;
                        break;
                    case DTK_MONTH:
                        if (!AdjustMonths(val, itm_in) || !AdjustFractDays(fval, DAYS_PER_MONTH, itm_in))
                            return DTERR_FIELD_OVERFLOW;
                        break;
                    case DTK_DAY:
                        if (!AdjustDays(val, 1, itm_in) || !AdjustFractMicroseconds(fval, USECS_PER_DAY, itm_in))
                            return DTERR_FIELD_OVERFLOW;
                        break;
                    case DTK_HOUR:
                        if (!AdjustMicroseconds(val, fval, USECS_PER_HOUR, itm_in))
                            return DTERR_FIELD_OVERFLOW;
                        break;
                    // Additional time units handled similarly...
                }
                break;

            case DTK_STRING:
            case DTK_SPECIAL:
                // Handle unit names and special keywords
                type = DecodeUnits(i, field[i], &uval);
                if (type == UNKNOWN_FIELD)
                    type = DecodeSpecial(i, field[i], &uval);

                if (type == UNITS) {
                    parsing_unit_val = true;
                } else if (type == AGO) {
                    if (i != nf - 1) return DTERR_BAD_FORMAT;  // AGO must be last
                    is_before = true;
                } else if (type == RESERV) {
                    // Handle infinite intervals
                    if (uval != DTK_LATE && uval != DTK_EARLY) return DTERR_BAD_FORMAT;
                    if (i != nf - 1) return DTERR_BAD_FORMAT;  // Must be last
                    *dtype = uval;
                }
                break;
        }

        // Check for duplicate field masks
        if (tmask & fmask) return DTERR_BAD_FORMAT;
        fmask |= tmask;
    }

    // Apply AGO negation to all components
    if (is_before) {
        itm_in->tm_usec = -itm_in->tm_usec;
        itm_in->tm_mday = -itm_in->tm_mday;
        itm_in->tm_mon = -itm_in->tm_mon;
        itm_in->tm_year = -itm_in->tm_year;
    }

    return 0;
}
```