# ParseDateTime

## Location
src/interfaces/ecpg/pgtypeslib/dt_common.c: 1598 - 1779

## Overview
ParseDateTime breaks input date/time strings into tokens based on context, identifying field types and extracting individual components for further processing.

## Definition
```c
int ParseDateTime(const char *timestr, char *workbuf, size_t buflen,
                 char **field, int *ftype, int maxfields, int *numfields)
```

## Detailed Description
ParseDateTime is a fundamental tokenization function that breaks down date/time input strings into individual fields with type classification. It processes various formats including numbers, dates, times, text strings, special tokens, and timezone specifications. The function handles multiple delimiters and formats, converts text to lowercase, and assigns specific field types (DTK_NUMBER, DTK_DATE, DTK_TIME, DTK_STRING, DTK_SPECIAL, DTK_TZ) to guide subsequent parsing stages. This is typically the first stage in PostgreSQL's comprehensive date/time parsing pipeline.

## Parameters / Member Variables
- `timestr`: Input date/time string to be tokenized
- `workbuf`: Workspace buffer for storing extracted field strings (must be larger than input)
- `buflen`: Size of the workspace buffer
- `field[]`: Output array of pointers to extracted field strings
- `ftype[]`: Output array of field type indicators (DTK_* constants)
- `maxfields`: Maximum number of fields that can be stored in field[] and ftype[] arrays
- `*numfields`: Output parameter set to actual number of fields detected

## Dependencies
- Functions called/Symbols referenced:
  - DTERR_BAD_FORMAT
  - DTK_TIME, DTK_DATE, DTK_NUMBER, DTK_STRING, DTK_SPECIAL, DTK_TZ (field type constants)
  - [pg_tolower](../p/pg_tolower.md)
  - [datebsearch](../d/datebsearch.md)
  - datetktbl, szdatetktbl (date token table)
- Called from (representative examples):
  - [date_in](../d/date_in.md), time_in, timetz_in
  - [timestamp_in](../t/timestamp_in.md), timestamptz_in
  - [interval_in](../i/interval_in.md)
  - [check_recovery_target_time](../c/check_recovery_target_time.md)
  - ECPG datetime parsing functions

## Notes and Other Information
- Core tokenization function used throughout PostgreSQL's datetime input processing
- Handles complex field type detection including timezone names and embedded delimiters
- Performs case conversion to lowercase for consistent processing
- Field types can hold unexpected items (e.g., DTK_NUMBER can hold date fields like yy.ddd)
- Used by both backend and ECPG client library datetime processing
- Returns 0 on success, DTERR_BAD_FORMAT on invalid input
- Critical for parsing diverse date/time input formats accepted by PostgreSQL