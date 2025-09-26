# DateTimeParseError

## Location
src/backend/utils/adt/datetime.c: 4092 - 4152

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
  - DateTimeErrorExtra (auxiliary error information)
  - Node (PostgreSQL node system)
- Error codes handled:
  - DTERR_FIELD_OVERFLOW, DTERR_MD_FIELD_OVERFLOW
  - DTERR_INTERVAL_OVERFLOW, DTERR_TZDISP_OVERFLOW
  - DTERR_BAD_TIMEZONE, DTERR_BAD_ZONE_ABBREV
  - DTERR_BAD_FORMAT
- Called from (representative examples):
  - date_in, time_in, timetz_in (date.c)
  - timestamp_in, timestamptz_in, interval_in (timestamp.c)
  - DecodeTimezoneName, do_to_timestamp (datetime.c, formatting.c)

## Notes and Other Information
- Implements SQL99-compliant SQLSTATE codes for different error types
- Supports both traditional exception throwing and PostgreSQL's soft error handling
- The extra parameter is only used for timezone-related errors (BAD_TIMEZONE, BAD_ZONE_ABBREV)
- MD_FIELD_OVERFLOW errors specifically suggest checking DateStyle configuration
- Default case handles DTERR_BAD_FORMAT and any unrecognized error codes
- Function is void and does not return - either throws an error or fills escontext