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
  - DatumGetDateADT, DatumGetTimeADT, DatumGetTimeTzADTP, DatumGetTimestamp, DatumGetTimestampTz
  - DATE_NOT_FINITE, TIMESTAMP_NOT_FINITE
  - j2date, time2tm, timetz2tm, timestamp2tm
  - EncodeSpecialDate, EncodeSpecialTimestamp, EncodeDateOnly, EncodeTimeOnly, EncodeDateTime
  - USE_XSD_DATES, MAXDATELEN, POSTGRES_EPOCH_JDATE, USECS_PER_SEC
- Called from (representative examples):
  - datum_to_json_internal
  - datum_to_jsonb_internal
  - convertJsonbScalar
  - executeItemOptUnwrapTarget

## Notes and Other Information
The function ensures JSON compatibility by using XSD date format standards and handles PostgreSQL's special datetime values (infinity, -infinity). For timestamptz with custom timezone, it applies the offset before conversion and sets the tm_isdst flag to indicate timezone presence. Error handling is provided for out-of-range timestamps and unknown datetime type OIDs.