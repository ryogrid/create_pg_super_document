# GetMessageEncoding

## Location
src/backend/utils/mb/mbutils.c: 1308 - 1324

## Overview
Returns the encoding ID used by gettext() for localized error messages and system messages, which may differ from the database client encoding.

## Definition
```c
int GetMessageEncoding(void)
```

## Detailed Description
This function returns the encoding ID that gettext() uses for localized messages. The message encoding often matches the database encoding, but differs in several important cases:
- SQL_ASCII databases (where message encoding remains in locale encoding)
- Processes not attached to a database
- Database encodings that lack iconv support (such as MULE_INTERNAL)

The function simply returns the encoding field from the global MessageEncoding variable, which points to an entry in the pg_enc2name_tbl table. MessageEncoding is initialized to PG_SQL_ASCII and updated when the message encoding is explicitly set.

## Parameters / Member Variables
- No parameters
- Returns: int representing the encoding ID for messages

## Dependencies
- Functions called/Symbols referenced:
  - MessageEncoding - global static variable pointing to current message encoding info

- Called from (representative examples):
  - write_eventlog - Windows event log writing (src/backend/utils/error/elog.c:2543)
  - pg_bind_textdomain_codeset - text domain binding (src/backend/utils/mb/mbutils.c:1248)
  - pgwin32_message_to_UTF16 - Windows message conversion (src/backend/utils/mb/mbutils.c:1776)

## Notes and Other Information
- MessageEncoding is a static global variable in mbutils.c initialized to PG_SQL_ASCII
- The message encoding can differ from client encoding for better localization support
- Used primarily on Windows for proper Unicode message handling in event logs
- Critical for proper character encoding in error messages and system notifications
- Function signature location: src/backend/utils/mb/mbutils.c:1308-1324
- The function includes detailed comments explaining when message encoding differs from database encoding