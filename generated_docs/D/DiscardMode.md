# DiscardMode

## Location
[src/include/nodes/parsenodes.h:3930-3931](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3930-L3931)

## Overview
DiscardMode is an enumeration type that defines the different types of cached information that can be discarded using the PostgreSQL DISCARD SQL statement to free up session-specific resources.

## Definition

```c
typedef enum DiscardMode
{
	DISCARD_ALL,
	DISCARD_PLANS,
	DISCARD_SEQUENCES,
	DISCARD_TEMP,
} DiscardMode;
```
## Detailed Description
DiscardMode specifies what type of session state should be cleared by the DISCARD statement. The DISCARD statement is used to free up session-local resources and reset various session states to help with connection pooling and session cleanup:

- **DISCARD_ALL**: Discards all session state including prepared statements, temporary tables, sequences, and plans. This is the most comprehensive cleanup option.

- **DISCARD_PLANS**: Discards all cached query plans (prepared statements). This forces recompilation of prepared statements on next execution.

- **DISCARD_SEQUENCES**: Discards cached sequence values, forcing the next sequence operations to fetch fresh values from the sequence objects.

- **DISCARD_TEMP**: Discards temporary tables and related objects created in the current session.

This functionality is particularly useful for connection pooling scenarios where database connections are reused across different application sessions and need to be reset to a clean state.

## Parameters / Member Variables
- `DISCARD_ALL`: Discard all session state (comprehensive cleanup)
- `DISCARD_PLANS`: Discard cached query plans and prepared statements
- `DISCARD_SEQUENCES`: Discard cached sequence values
- `DISCARD_TEMP`: Discard temporary tables and objects

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is an enumeration type)
- Called from (representative examples):
  - DiscardStmt (src/include/nodes/parsenodes.h:3935)

## Notes and Other Information
- Primarily used in connection pooling scenarios to reset session state between different users
- DISCARD ALL is equivalent to executing DISCARD PLANS, DISCARD SEQUENCES, and DISCARD TEMP together
- Essential for security in multi-tenant environments where database connections are shared
- Helps prevent information leakage between sessions in pooled connection scenarios
- Part of PostgreSQL's session management and resource cleanup infrastructure
- Does not affect transaction state - transactions must be explicitly committed or rolled back before using DISCARD