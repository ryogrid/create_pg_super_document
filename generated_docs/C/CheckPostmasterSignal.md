# CheckPostmasterSignal

## Location
[src/backend/storage/ipc/pmsignal.c:198-217](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/pmsignal.c#L198-L217)

## Overview
Checks if a specific signal reason has been set by a child process and atomically clears the flag, typically called by the postmaster after receiving SIGUSR1.

## Definition

```c
bool
CheckPostmasterSignal(PMSignalReason reason)
```
## Detailed Description
CheckPostmasterSignal is the counterpart to SendPostmasterSignal, designed to be called by the postmaster process when it receives a SIGUSR1 signal. The function performs an atomic test-and-clear operation on the specified signal flag in shared memory:

1. Checks if the specified reason flag is set in the PMSignalState->PMSignalFlags array
2. If the flag is set, it clears the flag (sets it to false) and returns true
3. If the flag is not set, it returns false without modifying anything

The careful design ensures that each signal flag is processed exactly once - the flag is cleared only if it was actually set, preventing spurious processing and ensuring reliable signal delivery semantics.

## Parameters / Member Variables
- : PMSignalReason enum value indicating which specific signal flag to check and clear
- Returns: bool - true if the flag was set (and has now been cleared), false if it was not set

## Dependencies
- Functions called/Symbols referenced:
  - PMSignalReason (enum type for signal reasons)
  - PMSignalState (global shared memory structure)
- Called from (representative examples):
  - [process_pm_pmsignal](../p/process_pm_pmsignal.md) (postmaster signal processing - multiple calls for different reasons)

## Notes and Other Information
- This is a public function intended to be called only by the postmaster process
- Implements atomic test-and-clear semantics to prevent double-processing of signals
- Typically called from the postmaster's SIGUSR1 signal handler or related processing
- The comment emphasizes the careful design to avoid clearing flags that weren't actually set
- Part of PostgreSQL's inter-process communication mechanism between postmaster and backends
- Each PMSignalReason enum value corresponds to a different type of event or request from child processes

## Simplified Source

```c
// Simplified version of CheckPostmasterSignal
bool CheckPostmasterSignal(PMSignalReason reason) {
    // Check if the signal flag is set for this reason
    if (PMSignalState->PMSignalFlags[reason]) {
        // Clear the flag atomically and return true
        PMSignalState->PMSignalFlags[reason] = false;
        return true;
    }

    // Flag was not set, return false without modifying anything
    return false;
}
```

Key simplifications made:
- Added explanatory comments for each logical step
- Emphasized the atomic test-and-clear operation
- Highlighted the careful design that only clears flags that were actually set
- Focused on the core logic: check flag, clear if set, return status