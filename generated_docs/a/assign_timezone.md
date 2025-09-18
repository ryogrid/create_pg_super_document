# assign_timezone

## Location
src/backend/commands/variable.c: 381 - 389

## Overview
A GUC (Grand Unified Configuration) assignment hook function that applies a validated timezone configuration by setting the global session_timezone variable.

## Definition


## Detailed Description
The  function serves as a GUC assign hook that applies timezone configuration changes that were previously validated by . It extracts the validated pg_tz timezone object from the extra data structure and assigns it to the global  variable, which controls the timezone context for the current database session.

This function is called by the GUC system after successful validation to make the timezone change effective throughout the session. It's a simple assignment function that trusts the validation has already been performed by the corresponding check hook.

## Parameters / Member Variables
- : The string representation of the timezone setting (unused in this function since the timezone object comes from extra)
- : A pointer to the extra data structure containing the validated pg_tz timezone object (created by check_timezone)

## Dependencies
- Functions called/Symbols referenced:
  - session_timezone: Global variable storing the current session's timezone context
  - [pg_tz](../p/pg_tz.md): PostgreSQL timezone object type

- Called from (representative examples):
  - GUC system during timezone configuration assignment after successful validation

## Notes and Other Information
- This function is always called after check_timezone has successfully validated the input
- The extra parameter contains a single pg_tz pointer to the validated timezone object
- No error checking is performed since validation was done in the check hook
- The function directly modifies the global session_timezone variable that affects all time-related operations in the current session
- This is part of PostgreSQL's GUC (Grand Unified Configuration) system architecture where check and assign hooks are paired
- The session_timezone variable is used throughout PostgreSQL for timezone conversions and display formatting