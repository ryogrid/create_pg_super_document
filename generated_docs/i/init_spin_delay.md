# init_spin_delay

## Location
[src/include/storage/s_lock.h:832-842](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/s_lock.h#L832-L842)

## Overview
The `init_spin_delay` function initializes a SpinDelayStatus structure for tracking spinlock delay statistics and debugging information.

## Definition
```c
static inline void init_spin_delay(SpinDelayStatus *status, const char *file, int line, const char *func)
```

## Detailed Description
The `init_spin_delay` function initializes a SpinDelayStatus structure by resetting all counters to zero and storing debugging information about the location where the spinlock operation is being initiated. This function is part of PostgreSQL's spinlock delay tracking infrastructure, which helps monitor and debug spinlock contention by recording statistics about spin counts, delays, and the source location of spinlock operations.

## Parameters / Member Variables
- `status`: Pointer to a SpinDelayStatus structure to be initialized
- `file`: Source file name where the spinlock operation is initiated (__FILE__)
- `line`: Line number in the source file where the spinlock operation is initiated (__LINE__)
- `func`: Function name where the spinlock operation is initiated (__func__)

## Dependencies
- Functions called/Symbols referenced:
  - SpinDelayStatus (structure type)
- Called from (representative examples):
  - [s_lock](../s/s_lock.md) function in s_lock.c
  - init_local_spin_delay macro

## Notes and Other Information
- Part of PostgreSQL's spinlock performance monitoring and debugging infrastructure
- Initializes counters (spins, delays, cur_delay) to zero
- Stores source location information for debugging spinlock contention issues
- Typically used in conjunction with other spinlock delay tracking functions
- Helps identify hot spots and performance bottlenecks in spinlock usage
- The debugging information is valuable for troubleshooting high-contention scenarios

## Simplified Source

```c
static inline void
init_spin_delay(SpinDelayStatus *status,
                const char *file, int line, const char *func)
{
    // Initialize all counters to zero
    status->spins = 0;
    status->delays = 0;
    status->cur_delay = 0;

    // Store debugging information for contention analysis
    status->file = file;
    status->line = line;
    status->func = func;
}
```