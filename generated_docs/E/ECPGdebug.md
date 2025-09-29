# ECPGdebug

## Location
[src/interfaces/ecpg/ecpglib/misc.c:204-231](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/misc.c#L204-L231)

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

## Simplified Source

```c
void ECPGdebug(int n, FILE *dbgs)
{
    // Lock to prevent concurrent debug configuration changes
    pthread_mutex_lock(&debug_init_mutex);
    pthread_mutex_lock(&debug_mutex);

    // Check for regression test mode (n > 100)
    if (n > 100)
    {
        ecpg_internal_regression_mode = true;
        simple_debug = n - 100;  // Use actual debug level minus 100
    }
    else
        simple_debug = n;

    // Set the debug output stream
    debugstream = dbgs;

    // Release debug_mutex before logging to avoid deadlock
    pthread_mutex_unlock(&debug_mutex);

    // Log the debug level change (still holding debug_init_mutex)
    ecpg_log("ECPGdebug: set to %d\n", simple_debug);

    pthread_mutex_unlock(&debug_init_mutex);
}
```