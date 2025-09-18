# PGTYPEStimestamp_defmt_asc

## Location
src/interfaces/ecpg/pgtypeslib/timestamp.c: 810 - 861

## Overview
Parses an ASCII string representation of a timestamp according to a specified format string and converts it into a PostgreSQL timestamp value.

## Definition
```c
int PGTYPEStimestamp_defmt_asc(const char *str, const char *fmt, timestamp *d)
```

## Detailed Description
This function performs the reverse operation of timestamp formatting - it takes a formatted string representation of a timestamp and parses it according to a provided format specification to produce a timestamp value. The function uses a default format of "%Y-%m-%d %H:%M:%S" if no format is provided. It initializes date/time components with sentinel values to detect which fields were actually specified in the input string, then delegates the actual parsing work to PGTYPEStimestamp_defmt_scan. The function handles memory management by creating copies of the input strings and freeing them after processing.

## Parameters / Member Variables
- `str`: Input string containing the timestamp representation to be parsed
- `fmt`: Format string specifying how to interpret the input string (NULL uses default format)
- `d`: Pointer to the timestamp variable where the parsed result will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [pgtypes_strdup](../p/pgtypes_strdup.md)
  - [PGTYPEStimestamp_defmt_scan](PGTYPEStimestamp_defmt_scan.md)
- Called from (representative examples):
  - [dtcvfmtasc](../d/dtcvfmtasc.md) (in compatlib)
  - [main](../m/main.md) (extensively in test cases)

## Notes and Other Information
- Returns an integer status code indicating parsing success or failure
- Uses default format "%Y-%m-%d %H:%M:%S" when fmt is NULL
- Initializes date/time components with sentinel values (-1) to detect unspecified fields
- Creates temporary copies of input strings for safe processing
- Extensively tested with various date/time format combinations
- Part of the ECPG pgtypes library for embedded SQL applications