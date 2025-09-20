# pg_event_trigger_table_rewrite_reason

## Location
[src/backend/commands/event_trigger.c:1514-1553](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/event_trigger.c#L1514-L1553)

## Overview
A PostgreSQL built-in function that returns the reason code for a table rewrite operation, available only within table_rewrite event trigger functions.

## Definition

```c
struct CollectedCommand representation of itself to the command list,
 * using the routines below.
 *
 * 2) Some time after that, ddl_command_end fires and the command list is made
 * available to the event trigger function via pg_event_trigger_ddl_commands();
```
## Detailed Description
This function provides access to the reason code that indicates why a table rewrite operation was triggered. It can only be called from within table_rewrite event trigger functions and serves as a diagnostic tool for event trigger functions to understand the specific cause of the table rewrite.

The function enforces strict calling context validation to ensure it's used only when table rewrite information is available and meaningful. It checks that there is an active event trigger state and that the table_rewrite_reason field contains a non-zero value. If called outside the proper context, it raises a protocol violation error.

The reason code returned is an integer that corresponds to specific table rewrite scenarios. Table rewrites can be triggered by various DDL operations such as:
- ALTER TABLE operations that require data reorganization
- Column type changes that need data conversion  
- Adding columns with non-null defaults
- Changing table storage parameters
- Other structural modifications requiring physical data reorganization

## Parameters / Member Variables
- No direct parameters (uses PG_FUNCTION_ARGS macro)  
- Returns: Integer reason code indicating why the table rewrite was triggered

## Dependencies
- Functions called/Symbols referenced:
  - PG_RETURN_INT32 (macro for returning 32-bit integer values)
  - ereport, errcode, errmsg (error reporting functions)
  - ERRCODE_E_R_I_E_EVENT_TRIGGER_PROTOCOL_VIOLATED (error code constant)
- Called from:
  - No direct references (invoked by SQL as built-in function)

## Notes and Other Information
- Accessible only within table_rewrite event trigger functions - raises protocol violation error if called elsewhere
- Returns the table_rewrite_reason field from the current event trigger state
- Provides diagnostic information about the cause of table rewrite operations
- Reason codes are integer values that correspond to specific rewrite scenarios
- Useful for implementing conditional logic in event triggers based on rewrite type
- Complements pg_event_trigger_table_rewrite_oid by providing the 'why' while that function provides the 'what'
- Located in src/backend/commands/event_trigger.c:1514-1553
- Simple validation and accessor function with focused responsibility