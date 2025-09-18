# prompt1_hook

## Location
src/bin/psql/startup.c: 1098 - 1104

## Overview
A validation and assignment hook function for the PROMPT1 psql variable that updates the primary prompt string used by psql.

## Definition
static bool prompt1_hook(const char *newval)

## Detailed Description
This function serves as a validation hook for the PROMPT1 psql variable. Unlike other hook functions in psql, this one is very simple and always succeeds. It directly assigns the provided value to pset.prompt1, or sets it to an empty string if a NULL value is provided. The function does not perform any validation since prompt strings can contain arbitrary text and formatting sequences. This hook ensures that the primary prompt display is updated immediately when the PROMPT1 variable is changed.

## Parameters / Member Variables
- `newval`: The new string value being assigned to the PROMPT1 variable. Can be any string including NULL, which is converted to an empty string.

## Dependencies
- Functions called/Symbols referenced:
  - (none - only performs direct assignment)
- Called from (representative examples):
  - [EstablishVariableSpace](../E/EstablishVariableSpace.md) (via SetVariableHooks for PROMPT1 variable)

## Notes and Other Information
- Always returns true since prompt strings require no validation
- Handles NULL input by converting it to an empty string rather than leaving it NULL
- The function does not use a substitute hook (NULL is passed for substitute hook in SetVariableHooks)
- The pset.prompt1 field is used by psql's command line interface to display the primary prompt
- Prompt strings can contain special formatting sequences that are expanded when displayed
- This is the simplest of the psql variable hooks, requiring no error handling or complex validation
- Located in src/bin/psql/startup.c:1098-1104