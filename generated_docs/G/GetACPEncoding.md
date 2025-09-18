# GetACPEncoding

## Location
[src/backend/utils/error/elog.c:2472-2485](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L2472-L2485)

## Overview
GetACPEncoding is a Windows-specific static function that retrieves the PostgreSQL encoding equivalent of the Windows ANSI Code Page (ACP) for use with Windows ANSI system interfaces.

## Definition
```c
static int GetACPEncoding(void)
```

## Detailed Description
This function serves as a bridge between Windows system encoding and PostgreSQL's internal encoding system. It provides a cached lookup of the PostgreSQL encoding ID that corresponds to the current Windows ANSI Code Page. The ANSI Code Page is used by Windows "ANSI" system interfaces (such as CreateFileA()) which expect string arguments in this specific encoding.

The function uses lazy initialization with caching - it only queries the system once and stores the result for subsequent calls. This is efficient since the ANSI Code Page remains constant for all processes in a given Windows system.

## Parameters / Member Variables
- No parameters (void function)
- Returns: PostgreSQL encoding ID corresponding to the Windows ACP

## Dependencies
- Functions called/Symbols referenced:
  - GetACP (Windows API function to get current ANSI code page)
  - pg_codepage_to_encoding (PostgreSQL function to convert Windows code page to PostgreSQL encoding)
- Called from (representative examples):
  - [write_eventlog](../w/write_eventlog.md)

## Notes and Other Information
- Windows-specific function, only compiled and used on Windows platforms
- Uses static variable 'encoding' initialized to -2 for one-time lazy initialization
- The -2 initial value distinguishes uninitialized state from valid encoding IDs
- Part of PostgreSQL's Windows-specific logging infrastructure
- Essential for proper character encoding when writing to Windows Event Log
- Located in src/backend/utils/error/elog.c alongside other logging functions