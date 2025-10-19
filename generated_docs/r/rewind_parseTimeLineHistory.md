# rewind_parseTimeLineHistory

## Location
[src/bin/pg_rewind/timeline.c:28-129](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/timeline.c#L28-L129)

## Overview
Parses a timeline history buffer to construct a list of timeline entries, providing a pg_rewind-specific implementation of timeline history parsing without backend dependencies.

## Definition
TimeLineHistoryEntry *rewind_parseTimeLineHistory(char *buffer, TimeLineID targetTLI, int *nentries)

## Detailed Description
This function is a copy-pasted version of the backend's readTimeLineHistory function, modified specifically for pg_rewind to work without backend functions and return a malloc'd array. It parses timeline history data from a buffer to construct a chronological list of timeline entries.

The function processes each line in the history buffer, extracting timeline IDs and their corresponding WAL switchpoints. It validates the data format and ensures timeline IDs are in increasing sequence. After parsing all existing timeline entries from the history, it creates an additional entry for the "tip" of the timeline (the current timeline that has no entry in the history file).

The parsing follows a strict format where each line contains a timeline ID followed by a WAL switchpoint location in hexadecimal format (high/low 32-bit values). Lines starting with '#' are treated as comments and skipped.

## Parameters / Member Variables
- : Input buffer containing the timeline history data to parse
- : The target timeline ID for which history is being parsed
- : Output parameter that receives the number of timeline entries found

## Dependencies
- Functions called/Symbols referenced:
  - [TimeLineHistoryEntry](../T/TimeLineHistoryEntry.md) (struct type)
  - pg_log_error_detail
  - [pg_realloc](../p/pg_realloc.md)
  - [pg_malloc](../p/pg_malloc.md)
- Called from (representative examples):
  - [getTimelineHistory](../g/getTimelineHistory.md) (in src/bin/pg_rewind/pg_rewind.c:884)

## Notes and Other Information
- This is a pg_rewind-specific adaptation of backend timeline parsing functionality
- The function exits with status 1 on any parsing errors or data validation failures
- Timeline IDs must be in strictly increasing sequence in the history file
- The target timeline ID must be greater than all timeline IDs found in the history
- Memory is allocated using pg_malloc/pg_realloc and should be freed by the caller
- The function creates an additional entry for the current timeline tip with InvalidXLogRecPtr as the end point

## Simplified Source

```c
TimeLineHistoryEntry *
rewind_parseTimeLineHistory(char *buffer, TimeLineID targetTLI, int *nentries)
{
    char *fline;
    TimeLineHistoryEntry *entry;
    TimeLineHistoryEntry *entries = NULL;
    int nlines = 0;
    TimeLineID lasttli = 0;
    XLogRecPtr prevend;
    char *bufptr;
    bool lastline = false;

    // Parse timeline history buffer line by line
    prevend = InvalidXLogRecPtr;
    bufptr = buffer;

    while (!lastline)
    {
        char *ptr;
        TimeLineID tli;
        uint32 switchpoint_hi, switchpoint_lo;
        int nfields;

        // Extract current line
        fline = bufptr;
        while (*bufptr && *bufptr != '\n')
            bufptr++;
        if (!(*bufptr))
            lastline = true;
        else
            *bufptr++ = '\0';

        // Skip whitespace and comments
        for (ptr = fline; *ptr; ptr++)
        {
            if (!isspace((unsigned char) *ptr))
                break;
        }
        if (*ptr == '\0' || *ptr == '#')
            continue;

        // Parse timeline ID and switchpoint: "tli\thi/lo"
        nfields = sscanf(fline, "%u\t%X/%X", &tli, &switchpoint_hi, &switchpoint_lo);

        // Validate format and data
        if (nfields < 1)
        {
            pg_log_error("syntax error in history file: %s", fline);
            exit(1);
        }
        if (nfields != 3)
        {
            pg_log_error("syntax error in history file: %s", fline);
            exit(1);
        }
        if (entries && tli <= lasttli)
        {
            pg_log_error("invalid data in history file: %s", fline);
            exit(1);
        }

        lasttli = tli;

        // Add new timeline entry
        nlines++;
        entries = pg_realloc(entries, nlines * sizeof(TimeLineHistoryEntry));

        entry = &entries[nlines - 1];
        entry->tli = tli;
        entry->begin = prevend;
        entry->end = ((uint64) switchpoint_hi) << 32 | (uint64) switchpoint_lo;
        prevend = entry->end;
    }

    // Validate target timeline ID
    if (entries && targetTLI <= lasttli)
    {
        pg_log_error("invalid data in history file");
        exit(1);
    }

    // Add final entry for current timeline tip
    nlines++;
    if (entries)
        entries = pg_realloc(entries, nlines * sizeof(TimeLineHistoryEntry));
    else
        entries = pg_malloc(1 * sizeof(TimeLineHistoryEntry));

    entry = &entries[nlines - 1];
    entry->tli = targetTLI;
    entry->begin = prevend;
    entry->end = InvalidXLogRecPtr;

    *nentries = nlines;
    return entries;
}
```