# DateTimeParseError

## Location
[src/backend/utils/adt/datetime.c:4092-4152](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L4092-L4152)

## Overview
Reports detailed error messages for various types of datetime input processing errors, with support for both exception throwing and soft error handling.

## Definition
```c
void DateTimeParseError(int dterr, DateTimeErrorExtra *extra,
                       const char *str, const char *datatype,
                       Node *escontext)
```

## Detailed Description
This function centralizes error reporting for all datetime input processing routines in PostgreSQL. It translates internal DTERR error codes into appropriate PostgreSQL error messages with specific SQLSTATE codes as required by the SQL99 standard.

The function supports PostgreSQL's soft error handling mechanism through the escontext parameter. When escontext points to an ErrorSaveContext node, error information is stored there instead of throwing an exception, allowing callers to handle errors gracefully.

Different error types produce specialized error messages:
- Field overflow errors include the problematic input string
- Month/day field overflow errors suggest checking DateStyle settings
- Timezone-related errors provide specific timezone names from the extra parameter
- Bad format errors indicate invalid input syntax for the specified datatype

## Parameters / Member Variables
- `dterr`: Internal error code indicating the type of parsing error that occurred
- `extra`: Pointer to DateTimeErrorExtra structure containing auxiliary error information (nullable)
- `str`: Original input string that caused the error (used in error messages)
- `datatype`: Name of the PostgreSQL datatype being parsed (e.g., "timestamp", "date")
- `escontext`: ErrorSaveContext node for soft error handling, or NULL for normal exception throwing

## Dependencies
- Functions called/Symbols referenced:
  - errsave (PostgreSQL error handling)
- Structures used:
  - [DateTimeErrorExtra](DateTimeErrorExtra.md) (auxiliary error information)
  - [Node](../N/Node.md) (PostgreSQL node system)
- Error codes handled:
  - DTERR_FIELD_OVERFLOW, DTERR_MD_FIELD_OVERFLOW
  - DTERR_INTERVAL_OVERFLOW, DTERR_TZDISP_OVERFLOW
  - DTERR_BAD_TIMEZONE, DTERR_BAD_ZONE_ABBREV
  - DTERR_BAD_FORMAT
- Called from (representative examples):
  - [date_in](../d/date_in.md), time_in, timetz_in (date.c)
  - [timestamp_in](../t/timestamp_in.md), timestamptz_in, interval_in (timestamp.c)
  - [DecodeTimezoneName](DecodeTimezoneName.md), do_to_timestamp (datetime.c, formatting.c)

## Notes and Other Information
- Implements SQL99-compliant SQLSTATE codes for different error types
- Supports both traditional exception throwing and PostgreSQL's soft error handling
- The extra parameter is only used for timezone-related errors (BAD_TIMEZONE, BAD_ZONE_ABBREV)
- MD_FIELD_OVERFLOW errors specifically suggest checking DateStyle configuration
- Default case handles DTERR_BAD_FORMAT and any unrecognized error codes
- Function is void and does not return - either throws an error or fills escontext

## Simplified Source

```c
void
DateTimeParseError(int dterr, DateTimeErrorExtra *extra,
                   const char *str, const char *datatype,
                   Node *escontext)
{
    switch (dterr) {
        case DTERR_FIELD_OVERFLOW:
            errsave(escontext,
                    (errcode(ERRCODE_DATETIME_FIELD_OVERFLOW),
                     errmsg("date/time field value out of range: \"%s\"", str)));
            break;

        case DTERR_MD_FIELD_OVERFLOW:
            errsave(escontext,
                    (errcode(ERRCODE_DATETIME_FIELD_OVERFLOW),
                     errmsg("date/time field value out of range: \"%s\"", str),
                     errhint("Perhaps you need a different \"datestyle\" setting.")));
            break;

        case DTERR_INTERVAL_OVERFLOW:
            errsave(escontext,
                    (errcode(ERRCODE_INTERVAL_FIELD_OVERFLOW),
                     errmsg("interval field value out of range: \"%s\"", str)));
            break;

        case DTERR_TZDISP_OVERFLOW:
            errsave(escontext,
                    (errcode(ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE),
                     errmsg("time zone displacement out of range: \"%s\"", str)));
            break;

        case DTERR_BAD_TIMEZONE:
            errsave(escontext,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("time zone \"%s\" not recognized", extra->dtee_timezone)));
            break;

        case DTERR_BAD_ZONE_ABBREV:
            errsave(escontext,
                    (errcode(ERRCODE_CONFIG_FILE_ERROR),
                     errmsg("time zone \"%s\" not recognized", extra->dtee_timezone),
                     errdetail("This time zone name appears in the configuration file for time zone abbreviation \"%s\".",
                               extra->dtee_abbrev)));
            break;

        case DTERR_BAD_FORMAT:
        default:
            errsave(escontext,
                    (errcode(ERRCODE_INVALID_DATETIME_FORMAT),
                     errmsg("invalid input syntax for type %s: \"%s\"", datatype, str)));
            break;
    }
}
```