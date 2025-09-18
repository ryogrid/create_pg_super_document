# print_msg

## Location
[src/bin/pg_ctl/pg_ctl.c:235-244](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_ctl/pg_ctl.c#L235-L244)

## Overview
A utility function in pg_ctl that conditionally prints localized messages to stdout based on the silent mode setting.

## Definition


## Detailed Description
The  function provides controlled output functionality for the pg_ctl utility. It takes an already-localized message string and prints it to stdout, but only when the global  flag is not set. This allows pg_ctl to suppress informational messages when running in silent mode while still allowing error messages to be output through other mechanisms.

The function is designed to handle pre-translated/localized strings, meaning internationalization should be performed before calling this function. It uses  to write the message and immediately flushes stdout to ensure the message is displayed promptly.

## Parameters / Member Variables
- : A pre-localized string message to be printed to stdout

## Dependencies
- Functions called/Symbols referenced:
  -  (standard C library)
  -  (standard C library)
  -  (global variable check)
- Called from (representative examples):
  -  (pg_ctl.c:980, 985, 986, 989, 995, 1004)
  -  (pg_ctl.c:1044, 1049, 1053, 1061, 1063)
  -  (pg_ctl.c:1109, 1114, 1123, 1124)
  -  (pg_ctl.c:1232, 1235, 1236, 1240, 1247)
  -  (pg_ctl.c:691)
  -  (pg_ctl.c:732)

## Notes and Other Information
- This is a static function, only available within pg_ctl.c
- Respects the silent_mode global flag to control output verbosity
- Messages passed to this function should already be localized/translated
- Used extensively throughout pg_ctl operations for user feedback
- Ensures immediate output by flushing stdout after each message
- Essential for providing user feedback during PostgreSQL server lifecycle operations