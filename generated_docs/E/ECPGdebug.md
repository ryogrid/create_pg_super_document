# ECPGdebug

## Location
src/interfaces/ecpg/ecpglib/misc.c: 204 - 231

## Overview
Configures debug output settings for the ECPG library with thread-safe initialization and special regression test mode handling.

## Definition
```c
void ECPGdebug(int n, FILE *dbgs)
```

## Detailed Description
ECPGdebug controls the debugging output level and destination stream for ECPG operations. It uses pthread mutexes to ensure thread-safe configuration changes, preventing race conditions when multiple threads might simultaneously modify debug settings. The function supports special regression test mode (when n > 100) which enables internal regression testing behavior and adjusts the actual debug level accordingly. All debug configuration changes are logged through ecpg_log for traceability.

## Parameters / Member Variables
- `n`: Debug level setting - values > 100 enable regression mode and set debug level to (n-100)
- `dbgs`: FILE pointer specifying the output stream for debug messages (typically stderr or a log file)

## Dependencies
- Functions called/Symbols referenced:
  - [pthread_mutex_lock](../p/pthread_mutex_lock.md)
  - [pthread_mutex_unlock](../p/pthread_mutex_unlock.md)
  - [ecpg_log](../e/ecpg_log.md)
- Called from (representative examples):
  - Widely used in ECPG test suite initialization
  - Various main functions across test programs
  - Test functions like varchar_1, varchar_5, varchar_6

## Notes and Other Information
- Thread-safe implementation using two mutexes: debug_init_mutex and debug_mutex
- Special regression mode when debug level > 100 (sets ecpg_internal_regression_mode = true)
- Universally used in ECPG test programs, typically called early in program initialization
- Debug messages are controlled by simple_debug global variable
- Located in src/interfaces/ecpg/ecpglib/misc.c at lines 204-231