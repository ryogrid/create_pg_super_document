# pg_current_logfile

## Location
src/backend/utils/adt/misc.c: 1000 - 1091

## Overview
A PostgreSQL function that reports the current log file path used by the log collector by reading the current_logfiles metadata file.

## Definition
```c
Datum pg_current_logfile(PG_FUNCTION_ARGS)
```

## Detailed Description
This function retrieves information about the current log file(s) being used by PostgreSQL's logging system. It reads the LOG_METAINFO_DATAFILE (typically "current_logfiles") which is maintained by the syslogger process and contains mappings between log formats and their corresponding file paths. The function supports filtering by log format and returns the file path of the matching log file.

The function supports three log formats:
- "stderr": Standard error logging format
- "csvlog": Comma-separated values logging format  
- "jsonlog": JSON structured logging format

When called without arguments, it returns the first log file found in the metadata file. When called with a specific log format argument, it returns the path for that format if it exists.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0 (optional): text - log format to filter by ("stderr", "csvlog", or "jsonlog")

## Dependencies
- Functions called/Symbols referenced:
  - PG_NARGS, PG_ARGISNULL, PG_GETARG_TEXT_PP, PG_RETURN_TEXT_P, PG_RETURN_NULL (PostgreSQL macros)
  - text_to_cstring
  - AllocateFile
  - FreeFile
  - cstring_to_text
  - LOG_METAINFO_DATAFILE (constant for metadata file path)
- Called from (representative examples):
  - pg_current_logfile_1arg (wrapper function)

## Notes and Other Information
- This is a PostgreSQL SQL-callable function (available as pg_current_logfile in SQL)
- Returns NULL if no matching log file is found or if the metadata file doesn't exist
- Handles platform-specific line endings (CRLF on Windows via _O_TEXT mode)
- Performs validation of the log format parameter against supported formats
- Includes error handling for corrupted metadata file content
- The metadata file format is: "format filepath\n" for each active log file
- Used by database administrators and monitoring tools to locate current log files