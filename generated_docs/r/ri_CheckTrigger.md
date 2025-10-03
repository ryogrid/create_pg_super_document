# ri_CheckTrigger

## Location
[src/backend/utils/adt/ri_triggers.c:2012-2057](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L2012-L2057)

## Overview
A validation function that ensures referential integrity trigger functions are called in the correct context with appropriate trigger event types and timing.

## Definition

```c
static void
ri_CheckTrigger(FunctionCallInfo fcinfo, const char *funcname, int tgkind)
```
## Detailed Description
This function performs comprehensive validation to ensure that referential integrity trigger functions are invoked correctly by the PostgreSQL trigger system. It validates three critical aspects: that the function was actually called as a trigger (not directly), that the trigger timing and granularity are correct (AFTER ROW), and that the trigger event type matches the expected operation (INSERT, UPDATE, or DELETE).

The function first verifies the call was made through the trigger manager using the CALLED_AS_TRIGGER macro. It then ensures the trigger is configured as an AFTER ROW trigger, which is the only valid configuration for referential integrity operations. Finally, it validates that the actual trigger event (INSERT/UPDATE/DELETE) matches the expected trigger kind specified by the tgkind parameter.

Any validation failure results in an error with the specific error code ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED, ensuring that referential integrity trigger configuration errors are properly reported and diagnosed.

## Parameters / Member Variables
- `fcinfo`: FunctionCallInfo structure containing the trigger call context and metadata
- `*funcname`: Name of the referential integrity trigger function being validated (used in error messages)
- `tgkind`: Expected trigger kind/type, specified using RI_TRIGTYPE_* constants (INSERT, UPDATE, or DELETE)
## Dependencies
- Functions called/Symbols referenced:
  - : Structure containing function call context
  - : Structure containing trigger-specific information extracted from fcinfo->context
  - : Macro to verify the function was called by the trigger manager
  - : Macro to check if trigger fires after the triggering event
  - : Macro to verify the trigger is row-level (not statement-level)
  - : Macro to check if trigger was fired by INSERT operation
  - : Macro to check if trigger was fired by UPDATE operation
  - : Macro to check if trigger was fired by DELETE operation
  - : Constant identifying INSERT trigger type
  - : Constant identifying UPDATE trigger type
  - : Constant identifying DELETE trigger type
  - : Function to report errors with specific error codes and messages

- Called from (representative examples):
  - : Foreign key constraint check for INSERT operations
  - : Foreign key constraint check for UPDATE operations
  - : Foreign key NO ACTION constraint for DELETE operations
  - : Foreign key RESTRICT constraint for DELETE operations
  - : Foreign key CASCADE constraint for DELETE operations
  - : Foreign key CASCADE constraint for UPDATE operations
  - : Foreign key SET NULL constraint for DELETE operations
  - : Foreign key SET DEFAULT constraint for DELETE operations

## Notes and Other Information
- This is a static function within ri_triggers.c, used as a common validation entry point for all referential integrity trigger functions
- Essential for ensuring referential integrity triggers are properly configured and prevent runtime errors
- The function enforces PostgreSQL's requirement that RI triggers must be AFTER ROW triggers
- Uses specific error codes (ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED) to enable proper error categorization and handling
- Critical for maintaining data integrity by preventing improper trigger configurations that could lead to inconsistent constraint enforcement
- The validation helps catch configuration errors early and provides clear error messages for debugging trigger setup issues