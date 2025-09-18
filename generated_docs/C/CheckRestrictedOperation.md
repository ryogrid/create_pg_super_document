# CheckRestrictedOperation

## Location
src/backend/tcop/utility.c: 459 - 498

## Overview
CheckRestrictedOperation is a security validation function that prevents execution of hazardous SQL commands within security-restricted operation contexts, protecting session-local state that lacks other protection mechanisms.

## Definition


## Detailed Description
This function serves as a security gate within PostgreSQL's utility command processing pipeline. It checks whether the current execution context is operating under security restrictions and blocks potentially dangerous commands from executing in such contexts. The function is designed to protect session-local state and resources for which there are no better-defined protection mechanisms beyond basic ownership checks.

When a security restriction is detected, the function immediately raises an ERROR with insufficient privilege status, preventing the command from proceeding. This is crucial for maintaining security boundaries in contexts such as security definer functions, row-level security policies, or other restricted execution environments.

## Parameters / Member Variables
- `cmdname`: A string containing the name of the SQL command being attempted (e.g., "PREPARE", "LISTEN", etc.) - used in error reporting to inform users which specific command was blocked

## Dependencies
- Functions called/Symbols referenced:
  - InSecurityRestrictedOperation (checks if currently in a security-restricted context)
  - ereport (reports the error with appropriate error code and message)
- Called from (representative examples):
  - standard_ProcessUtility (multiple call sites for different command types)

## Notes and Other Information
- This function is static to utility.c, indicating it's an internal implementation detail of the utility command processing system
- The function uses ERRCODE_INSUFFICIENT_PRIVILEGE to maintain consistency with PostgreSQL's error reporting standards
- The error message is translatable (marked with translator comment) to support internationalization
- This security check is applied selectively to commands that could potentially compromise security in restricted contexts, rather than being applied universally to all utility commands
- The function represents PostgreSQL's defense-in-depth security approach, adding an additional layer of protection beyond standard permission checks