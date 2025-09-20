# write_syslog

## Location
[src/backend/utils/error/elog.c:2360-2471](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L2360-L2471)

## Overview
The write_syslog function is responsible for writing PostgreSQL log messages to the system syslog facility, handling message splitting for long messages and providing sequence numbering to suppress duplicate messages.

## Definition

```c
static void
write_syslog(int level, const char *line)
```
## Detailed Description
This internal function sends log messages to the system syslog facility. It implements several important features:

1. **Lazy syslog initialization**: Opens the syslog connection on first use using openlog() with appropriate flags
2. **Message sequencing**: Adds a sequence number to each message to help syslog suppress duplicate entries
3. **Message splitting**: Handles long messages by splitting them into smaller chunks to work around syslog implementation limitations
4. **Smart line breaking**: Attempts to break messages at word boundaries and respects multibyte character boundaries
5. **Newline handling**: Properly processes embedded newlines in log messages

The function respects several configuration options including syslog_split_messages and syslog_sequence_numbers to control its behavior.

## Parameters / Member Variables
- : The syslog priority level (e.g., LOG_ERR, LOG_WARNING, LOG_INFO)
- : The null-terminated log message string to be written to syslog

## Dependencies
- Functions called/Symbols referenced:
  - openlog (system call)
  - syslog (system call)
  - strlen, strchr, memcpy (standard C library)
  - [pg_mbcliplen](../p/pg_mbcliplen.md) (PostgreSQL multibyte utility)
  - PG_SYSLOG_LIMIT (constant defining maximum syslog message size)
- Called from (representative examples):
  - [send_message_to_server_log](../s/send_message_to_server_log.md)

## Notes and Other Information
- The function maintains static state including openlog_done flag and sequence counter
- Uses PG_SYSLOG_LIMIT constant to determine when message splitting is necessary
- Implements intelligent word boundary detection to avoid breaking words when possible
- Handles multibyte character encodings properly through pg_mbcliplen
- Configuration controlled by syslog_split_messages and syslog_sequence_numbers global variables
- Part of PostgreSQL's error logging infrastructure in src/backend/utils/error/elog.c