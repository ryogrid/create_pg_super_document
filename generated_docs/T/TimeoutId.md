# TimeoutId

## Location
[src/include/utils/timeout.h:43-50](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/timeout.h#L43-L50)

## Overview
TimeoutId is an enumeration that defines identifiers for different timeout reasons in PostgreSQL, serving as a unified system to manage multiple types of timeouts through a single SIGALRM interrupt mechanism.

## Definition

```c
typedef void (*timeout_handler_proc) (void);
```
## Detailed Description
TimeoutId provides a comprehensive enumeration system for managing various timeout scenarios in PostgreSQL. The enum defines both built-in system timeouts and allows for user-defined timeout extensions. When multiple timeouts trigger simultaneously, they are serviced in the order defined by this enumeration, establishing a clear priority system. The timeout system multiplexes SIGALRM interrupts to handle multiple concurrent timeout conditions efficiently.

## Parameters / Member Variables
- : Timeout for receiving startup packets from clients
- : Timeout for deadlock detection and resolution
- : Timeout for acquiring locks on database objects
- : Timeout for SQL statement execution
- : Deadlock timeout specific to standby servers
- : General timeout for standby server operations
- : Lock timeout specific to standby servers
- : Timeout for idle transactions
- : Overall transaction timeout
- : Timeout for idle database sessions
- : Timeout for updating idle statistics
- : Timeout for checking client connections
- : Timeout for startup progress monitoring
- : First identifier available for user-defined timeouts
- : Maximum number of supported timeout types (USER_TIMEOUT + 10)

## Dependencies
- Functions called/Symbols referenced: None (enum definition)
- Called from (representative examples):
  - [timeout_params](../t/timeout_params.md) (timeout.c:28)
  - [find_active_timeout](../f/find_active_timeout.md) (timeout.c:96)
  - [enable_timeout_after](../e/enable_timeout_after.md) (timeout.c:560)
  - [disable_timeout](../d/disable_timeout.md) (timeout.c:685)
  - [RegisterTimeout](../R/RegisterTimeout.md) (timeout.c:505)
  - EnableTimeoutParams struct
  - [DisableTimeoutParams](../D/DisableTimeoutParams.md) struct

## Notes and Other Information
The TimeoutId enum serves as the foundation for PostgreSQL's timeout management system, allowing the database to handle various timing constraints through a unified interface. The ordering is significant as it determines service priority when multiple timeouts expire simultaneously. The system reserves space for up to 10 user-defined timeout types beyond the predefined system timeouts, providing extensibility for custom timeout handling requirements.