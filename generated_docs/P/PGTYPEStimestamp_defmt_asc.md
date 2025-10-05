# PGTYPEStimestamp_defmt_asc

## Location
[src/interfaces/ecpg/pgtypeslib/timestamp.c:810-861](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/timestamp.c#L810-L861)

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

## Simplified Source

```c
int
PGTYPEStimestamp_defmt_asc(const char *str, const char *fmt, timestamp *d)
{
    int year, month, day;
    int hour, minute, second;
    int tz;
    char *mstr, *mfmt;
    int result;

    // Use default format if none provided
    if (!fmt)
        fmt = "%Y-%m-%d %H:%M:%S";
    if (!fmt[0])
        return 1;

    // Create working copies of input strings
    mstr = pgtypes_strdup(str);
    mfmt = pgtypes_strdup(fmt);

    // Initialize components with sentinel values to detect unspecified fields
    year = -1; month = -1; day = -1;
    hour = 0; minute = -1; second = -1;
    tz = 0;

    // Parse the string according to format
    result = PGTYPEStimestamp_defmt_scan(&mstr, mfmt, d, &year, &month, &day,
                                         &hour, &minute, &second, &tz);

    // Clean up allocated memory
    free(mstr);
    free(mfmt);

    return result;
}
```