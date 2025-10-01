# JsonEncodeDateTime

## Location
[src/backend/utils/adt/json.c:301-421](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L301-L421)

## Overview
Encodes datetime values into JSON-compatible string format using ISO standards, supporting various PostgreSQL datetime types with optional timezone handling.

## Definition
```c
char *JsonEncodeDateTime(char *buf, Datum value, Oid typid, const int *tzp)
```

## Detailed Description
JsonEncodeDateTime converts PostgreSQL datetime values (DATE, TIME, TIMETZ, TIMESTAMP, TIMESTAMPTZ) into JSON-compatible string representations following ISO format standards. The function handles special values like infinity and ensures consistent formatting by forcing the use of XSD date styles. For timestamptz values, it supports custom timezone offsets through the tzp parameter, allowing conversion to specific timezones before encoding.

The function allocates a buffer if none is provided and uses a switch statement to handle each datetime type appropriately. It leverages PostgreSQL's existing datetime conversion utilities but forces ISO/XSD formatting for JSON compatibility.

## Parameters / Member Variables
- `buf`: Pre-allocated buffer for the result string (allocated automatically if NULL)
- `value`: PostgreSQL Datum containing the datetime value to encode
- `typid`: OID identifying the specific datetime type (DATEOID, TIMEOID, etc.)
- `tzp`: Optional pointer to timezone offset in seconds for timestamptz conversion

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetDateADT](../D/DatumGetDateADT.md), DatumGetTimeADT, DatumGetTimeTzADTP, DatumGetTimestamp, DatumGetTimestampTz
  - DATE_NOT_FINITE, TIMESTAMP_NOT_FINITE
  - [j2date](../j/j2date.md), time2tm, timetz2tm, timestamp2tm
  - [EncodeSpecialDate](../E/EncodeSpecialDate.md), EncodeSpecialTimestamp, EncodeDateOnly, EncodeTimeOnly, EncodeDateTime
  - USE_XSD_DATES, MAXDATELEN, POSTGRES_EPOCH_JDATE, USECS_PER_SEC
- Called from (representative examples):
  - [datum_to_json_internal](../d/datum_to_json_internal.md)
  - [datum_to_jsonb_internal](../d/datum_to_jsonb_internal.md)
  - [convertJsonbScalar](../c/convertJsonbScalar.md)
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md)

## Notes and Other Information
The function ensures JSON compatibility by using XSD date format standards and handles PostgreSQL's special datetime values (infinity, -infinity). For timestamptz with custom timezone, it applies the offset before conversion and sets the tm_isdst flag to indicate timezone presence. Error handling is provided for out-of-range timestamps and unknown datetime type OIDs.

## Simplified Source

```c
char *JsonEncodeDateTime(char *buf, Datum value, Oid typid, const int *tzp) {
    if (!buf)
        buf = palloc(MAXDATELEN + 1);

    switch (typid) {
        case DATEOID:
            {
                DateADT date = DatumGetDateADT(value);
                struct pg_tm tm;

                if (DATE_NOT_FINITE(date)) {
                    EncodeSpecialDate(date, buf);
                } else {
                    j2date(date + POSTGRES_EPOCH_JDATE,
                          &(tm.tm_year), &(tm.tm_mon), &(tm.tm_mday));
                    EncodeDateOnly(&tm, USE_XSD_DATES, buf);
                }
            }
            break;

        case TIMEOID:
            {
                TimeADT time = DatumGetTimeADT(value);
                struct pg_tm tt, *tm = &tt;
                fsec_t fsec;

                time2tm(time, tm, &fsec);
                EncodeTimeOnly(tm, fsec, false, 0, USE_XSD_DATES, buf);
            }
            break;

        case TIMETZOID:
            {
                TimeTzADT *time = DatumGetTimeTzADTP(value);
                struct pg_tm tt, *tm = &tt;
                fsec_t fsec;
                int tz;

                timetz2tm(time, tm, &fsec, &tz);
                EncodeTimeOnly(tm, fsec, true, tz, USE_XSD_DATES, buf);
            }
            break;

        case TIMESTAMPOID:
            {
                Timestamp timestamp = DatumGetTimestamp(value);
                struct pg_tm tm;
                fsec_t fsec;

                if (TIMESTAMP_NOT_FINITE(timestamp)) {
                    EncodeSpecialTimestamp(timestamp, buf);
                } else if (timestamp2tm(timestamp, NULL, &tm, &fsec, NULL, NULL) == 0) {
                    EncodeDateTime(&tm, fsec, false, 0, NULL, USE_XSD_DATES, buf);
                } else {
                    ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                                   errmsg("timestamp out of range")));
                }
            }
            break;

        case TIMESTAMPTZOID:
            {
                TimestampTz timestamp = DatumGetTimestampTz(value);
                struct pg_tm tm;
                int tz;
                fsec_t fsec;
                const char *tzn = NULL;

                // Apply custom timezone offset if provided
                if (tzp) {
                    tz = *tzp;
                    timestamp -= (TimestampTz) tz * USECS_PER_SEC;
                }

                if (TIMESTAMP_NOT_FINITE(timestamp)) {
                    EncodeSpecialTimestamp(timestamp, buf);
                } else if (timestamp2tm(timestamp, tzp ? NULL : &tz, &tm, &fsec,
                                       tzp ? NULL : &tzn, NULL) == 0) {
                    if (tzp)
                        tm.tm_isdst = 1;  // set timezone presence flag
                    EncodeDateTime(&tm, fsec, true, tz, tzn, USE_XSD_DATES, buf);
                } else {
                    ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                                   errmsg("timestamp out of range")));
                }
            }
            break;

        default:
            elog(ERROR, "unknown jsonb value datetime type oid %u", typid);
            return NULL;
    }

    return buf;
}
```