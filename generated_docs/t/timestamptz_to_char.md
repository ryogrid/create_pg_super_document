# timestamptz_to_char

## Location
[src/backend/utils/adt/formatting.c:4285-4325](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L4285-L4325)

## Overview
SQL callable function that formats a TIMESTAMPTZ (timestamp with time zone) value into a string according to a specified format template, implementing the `to_char(timestamptz, format)` SQL function.

## Definition
```c
Datum timestamptz_to_char(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides the SQL interface for formatting TIMESTAMPTZ (timestamp with time zone) values into human-readable strings. It serves as the PostgreSQL built-in function `to_char()` when applied to TIMESTAMPTZ data types.

The function is nearly identical to `timestamp_to_char()` but includes additional timezone handling capabilities. It validates inputs for empty format strings and non-finite timestamps, returning NULL for invalid cases. The key difference is in the `timestamp2tm()` call, which extracts timezone information (`&tz`) and timezone name (`&tmtcTzn(&tmtc)`) alongside the standard timestamp breakdown.

Like its timestamp counterpart, it manually calculates day-of-week and day-of-year values, then delegates formatting to `datetime_to_char_body()`. The timezone information becomes available for use in format codes that display timezone details.

## Parameters / Member Variables
- Uses PostgreSQL's PG_FUNCTION_ARGS convention:
  - Argument 0: `TimestampTz dt` - The timestamptz value to format  
  - Argument 1: `text *fmt` - The format string template containing formatting codes

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMESTAMP
  - PG_GETARG_TEXT_PP  
  - TIMESTAMP_NOT_FINITE
  - [timestamp2tm](timestamp2tm.md) (with timezone extraction)
  - [date2j](../d/date2j.md) (for day-of-week and day-of-year calculations)
  - ZERO_tmtc/tmtcTm/tmtcFsec/tmtcTzn (TmToChar manipulation macros)
  - COPY_tm
  - [datetime_to_char_body](../d/datetime_to_char_body.md)
  - PG_GET_COLLATION
- Called from (representative examples):
  - SQL queries using to_char() function with timestamptz arguments
  - Direct SQL function calls

## Notes and Other Information
- Returns NULL for empty format strings or non-finite (infinity) timestamps
- Extracts timezone offset and timezone name information for formatting
- Manually calculates tm_wday and tm_yday since timestamp2tm() doesn't provide them  
- Uses same Julian day calculation as timestamp_to_char for day-of-week: (julianday + 1) % 7
- Day-of-year calculated as difference from January 1st of the same year plus 1
- Part of PostgreSQL's public SQL function interface, accessible via SQL to_char() calls
- Timezone information enables format codes like TZ, OF, etc. to display timezone details
- Handles timestamp overflow with appropriate error reporting using ERRCODE_DATETIME_VALUE_OUT_OF_RANGE

## Simplified Source

```c
Datum timestamptz_to_char(PG_FUNCTION_ARGS) {
    TimestampTz dt = PG_GETARG_TIMESTAMP(0);
    text *fmt = PG_GETARG_TEXT_PP(1);
    TmToChar tmtc;
    int tz;
    struct pg_tm tt;
    struct fmt_tm *tm;
    int thisdate;

    // Return NULL for empty format or invalid timestamp
    if (VARSIZE_ANY_EXHDR(fmt) <= 0 || TIMESTAMP_NOT_FINITE(dt))
        PG_RETURN_NULL();

    // Initialize time conversion structure
    ZERO_tmtc(&tmtc);
    tm = tmtcTm(&tmtc);

    // Convert timestamptz to broken-down time with timezone info
    if (timestamp2tm(dt, &tz, &tt, &tmtcFsec(&tmtc), &tmtcTzn(&tmtc), NULL) != 0)
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                       errmsg("timestamp out of range")));

    // Calculate day of week and day of year (not provided by timestamp2tm)
    thisdate = date2j(tt.tm_year, tt.tm_mon, tt.tm_mday);
    tt.tm_wday = (thisdate + 1) % 7;
    tt.tm_yday = thisdate - date2j(tt.tm_year, 1, 1) + 1;

    // Copy time structure and delegate to formatting function
    COPY_tm(tm, &tt);

    text *res = datetime_to_char_body(&tmtc, fmt, false, PG_GET_COLLATION());
    if (!res)
        PG_RETURN_NULL();

    PG_RETURN_TEXT_P(res);
}
```