# assign_datestyle

## Location
[src/backend/commands/variable.c:244-260](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/variable.c#L244-L260)

## Overview
A GUC (Grand Unified Configuration) assignment hook function that applies validated datestyle configuration values to the global DateStyle and DateOrder variables.

## Definition


## Detailed Description
The  function serves as a GUC assign hook that actually applies the datestyle configuration changes that were previously validated by . It extracts the validated date style and order values from the extra data structure and assigns them to the global  and  variables that control PostgreSQL's date formatting behavior throughout the system.

This function is called by the GUC system after successful validation to make the configuration change effective. It's a simple assignment function that trusts the validation has already been performed by the corresponding check hook.

## Parameters / Member Variables
- : The canonical string representation of the datestyle setting (unused in this function since values come from extra)
- : A pointer to the extra data structure containing the validated integer values for date style and order (created by check_datestyle)

## Dependencies
- Functions called/Symbols referenced:
  - DateStyle: Global variable storing the current date output style
  - DateOrder: Global variable storing the current date field order

- Called from (representative examples):
  - GUC system during configuration assignment after successful validation

## Notes and Other Information
- This function is always called after check_datestyle has successfully validated the input
- The extra parameter contains a 2-element integer array: [dateStyle, dateOrder]
- No error checking is performed since validation was done in the check hook
- The function directly modifies global state variables that affect date formatting system-wide
- This is part of PostgreSQL's GUC (Grand Unified Configuration) system architecture where check and assign hooks are paired