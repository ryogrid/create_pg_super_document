# get_timeout_active

## Location
[src/backend/utils/misc/timeout.c:780-792](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/timeout.c#L780-L792)

## Overview
Returns the active status of a specified timeout, indicating whether the timeout is currently enabled and has not yet fired.

## Definition
```c
bool get_timeout_active(TimeoutId id)
```

## Detailed Description
This function provides a simple query mechanism to check if a particular timeout is currently active in the system. A timeout is considered active if it has been enabled and has not yet expired or been explicitly disabled. The function directly accesses the timeout's active flag from the global timeout array.

The function includes an important caveat about race conditions - since timeouts can fire asynchronously via signal handlers, the returned status could become outdated immediately after the function returns. Callers should be aware that this is a point-in-time check rather than a guarantee.

## Parameters / Member Variables
- `id`: TimeoutId specifying which timeout to check for active status

## Dependencies
- Functions called/Symbols referenced:
  - [TimeoutId](../T/TimeoutId.md) (timeout identifier type)
- Called from (representative examples):
  - [start_xact_command](../s/start_xact_command.md) (transaction command processing)
  - [assign_transaction_timeout](../a/assign_transaction_timeout.md) (transaction timeout configuration)
  - [PostgresMain](../P/PostgresMain.md) (main backend processing loop)
  - [enable_statement_timeout](../e/enable_statement_timeout.md) (statement timeout management)
  - [disable_statement_timeout](../d/disable_statement_timeout.md) (statement timeout management)
  - [DisableTimeoutParams](../D/DisableTimeoutParams.md) (macro wrapper)

## Notes and Other Information
- Subject to race conditions as acknowledged in the source comments
- Provides read-only access to timeout status information
- Returns boolean true if timeout is active, false otherwise
- Used primarily in timeout management logic to avoid redundant operations
- The function performs no validation of the TimeoutId parameter - callers must ensure valid timeout IDs