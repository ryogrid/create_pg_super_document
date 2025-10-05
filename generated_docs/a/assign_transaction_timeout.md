# assign_transaction_timeout

## Location
[src/backend/tcop/postgres.c:3683-3701](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L3683-L3701)

## Overview
A GUC assign hook function that manages the transaction timeout mechanism when the  configuration parameter is changed during runtime.

## Definition

```c
void
assign_transaction_timeout(int newval, void *extra)
```
## Detailed Description
This function serves as a GUC (Grand Unified Configuration) assign hook that is automatically invoked whenever the  parameter is modified. It ensures that transaction timeout behavior is properly managed within active transactions by enabling or disabling the timeout timer based on the new value. When called within an active transaction, it immediately applies the new timeout setting rather than waiting for the next transaction to begin.

## Parameters / Member Variables
- `newval`: The new value for the transaction_timeout parameter (in milliseconds). Values > 0 enable the timeout, while values <= 0 disable it.
- `*extra`: Additional context data passed by the GUC system (unused in this implementation)
## Dependencies
- Functions called/Symbols referenced:
  - [IsTransactionState](../I/IsTransactionState.md): Checks if currently within a transaction block
  - [get_timeout_active](../g/get_timeout_active.md): Determines if the TRANSACTION_TIMEOUT is currently active
  - [enable_timeout_after](../e/enable_timeout_after.md): Enables the transaction timeout with specified duration
  - [disable_timeout](../d/disable_timeout.md): Disables the transaction timeout
  - TRANSACTION_TIMEOUT: Timeout identifier constant
- Called from (representative examples):
  - GUC system (via function pointer in guc_hooks.h)

## Notes and Other Information
- This function only takes action when called within an active transaction state
- The timeout is managed through PostgreSQL's timeout management system
- Changes to transaction_timeout outside of transactions will be applied to future transactions automatically
- Part of the GUC hook mechanism that allows custom behavior when configuration parameters change

## Simplified Source

```c
void assign_transaction_timeout(int newval, void *extra)
{
    if (IsTransactionState())
    {
        // Enable timeout if new value is positive and not already active
        if (newval > 0 && !get_timeout_active(TRANSACTION_TIMEOUT))
            enable_timeout_after(TRANSACTION_TIMEOUT, newval);

        // Disable timeout if new value is zero/negative and currently active
        else if (newval <= 0 && get_timeout_active(TRANSACTION_TIMEOUT))
            disable_timeout(TRANSACTION_TIMEOUT, false);
    }
}
```