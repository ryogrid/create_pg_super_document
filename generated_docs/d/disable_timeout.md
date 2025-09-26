# disable_timeout

## Location
src/backend/utils/misc/timeout.c: 685 - 717

## Overview
Cancels a specific timeout, removing it from the active timeout list and optionally resetting its firing indicator.

## Definition


## Detailed Description
This function disables a previously enabled timeout by removing it from the active timeout queue. The function provides flexibility in handling the timeout's indicator flag, which tracks whether the timeout has fired. When keep_indicator is false, the indicator is reset, allowing for clean timeout state management. When true, the indicator preserves its state, which can be useful for checking if a timeout fired before being disabled.

The function safely handles the case where a timeout is already disabled (it's not an error), maintains the integrity of other active timeouts, and reschedules the system alarm if other timeouts remain active. This ensures the timeout system continues to function correctly for remaining active timeouts.

## Parameters / Member Variables
- `id`: TimeoutId specifying which timeout to disable
- `keep_indicator`: bool determining whether to preserve the timeout's fired indicator (true) or reset it (false)

## Dependencies
- Functions called/Symbols referenced:
  - disable_alarm
  - find_active_timeout
  - remove_timeout_index
  - GetCurrentTimestamp
  - schedule_alarm
- Called from (representative examples):
  - CommitTransaction
  - PrepareTransaction
  - AbortTransaction
  - disable_startup_progress_timeout
  - ProcSleep
  - BackendInitialize
  - assign_transaction_timeout
  - PostgresMain
  - enable_statement_timeout
  - disable_statement_timeout
  - PerformAuthentication
  - DisableTimeoutParams

## Notes and Other Information
- Safe to call on timeouts that are already disabled - not considered an error condition
- Includes assertion checks to ensure the timeout system is properly initialized
- Temporarily disables alarm interrupts during the disable operation for thread safety
- Automatically reschedules remaining active timeouts when one is disabled
- The keep_indicator parameter allows for timeout firing detection even after disabling
- Widely used throughout PostgreSQL for transaction management, authentication, statement timeouts, and process control
- Part of PostgreSQL's comprehensive timeout management system for coordinating time-sensitive operations