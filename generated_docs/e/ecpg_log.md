# ecpg_log

## Location
src/interfaces/ecpg/ecpglib/misc.c: 232 - 289

## Overview
Provides thread-safe debug logging functionality for the ECPG library with internationalization and process ID tracking support.

## Definition
```c
void ecpg_log(const char *format, ...)
```

## Detailed Description
ecpg_log is the central logging function for ECPG debug output. It supports printf-style variadic arguments and includes sophisticated features like message internationalization through ecpg_gettext, process ID insertion for multi-process debugging, and special regression test mode that produces consistent output. The function uses mutex locking to ensure thread-safe operation and includes performance optimizations like checking debug level without mutex acquisition initially. In regression mode, it also outputs sqlca (SQL Communications Area) state information for debugging.

## Parameters / Member Variables
- `format`: Printf-style format string for the log message
- `...`: Variable arguments corresponding to format specifiers in the format string

## Dependencies
- Functions called/Symbols referenced:
  - ECPGget_sqlca
  - ecpg_gettext
  - malloc
  - [pthread_mutex_lock](../p/pthread_mutex_lock.md)
  - [pthread_mutex_unlock](../p/pthread_mutex_unlock.md)
  - vfprintf
- Called from (representative examples):
  - [ecpg_finish](ecpg_finish.md), ECPGsetcommit, ECPGnoticeReceiver in connect.c
  - ecpg_get_data in data.c
  - [ECPGget_desc_header](../E/ECPGget_desc_header.md), ECPGget_desc in descriptor.c
  - [ecpg_raise](ecpg_raise.md), ecpg_check_PQresult in error.c
  - ecpg_execute, ecpg_process_output in execute.c
  - [ECPGtrans](../E/ECPGtrans.md), ECPGdebug in misc.c
  - prepare_common, ecpg_auto_prepare in prepare.c
  - [ecpg_build_compat_sqlda](ecpg_build_compat_sqlda.md), ecpg_set_compat_sqlda in sqlda.c

## Notes and Other Information
- Thread-safe implementation with debug_mutex protection
- Performance optimization: initial simple_debug check without mutex
- Supports internationalization via ecpg_gettext for error messages
- Process ID insertion (disabled in regression test mode)
- Special regression mode outputs sqlca state information
- Dynamically allocates format string buffer with PID/NO_PID prefix
- Widely used throughout ECPG library for debugging and error reporting
- Located in src/interfaces/ecpg/ecpglib/misc.c at lines 232-289