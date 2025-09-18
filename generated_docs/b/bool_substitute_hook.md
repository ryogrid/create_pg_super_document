# bool_substitute_hook

## Location
src/bin/psql/startup.c: 858 - 874

## Overview
A substitute hook function for psql boolean variables that normalizes variable assignments by converting unset or empty values to standardized "off" and "on" string representations.

## Definition


## Detailed Description
This function serves as a variable substitution hook in psql's variable management system, specifically designed to handle boolean variables. It implements the logic to normalize boolean variable assignments by converting NULL values (from \unset commands) to "off" and empty string values (from bare \set commands) to "on". This normalization ensures consistent boolean representation across psql's variable system and supports the expected behavior where \set VAR with no value means "turn on" and \unset VAR means "turn off".

## Parameters / Member Variables
- : The incoming variable value to be processed; may be NULL, empty string, or contain actual content

## Dependencies
- Functions called/Symbols referenced:
  - pg_strdup (string duplication)
  - pg_free (memory deallocation)
- Called from (representative examples):
  - EstablishVariableSpace (multiple variable registrations)

## Notes and Other Information
- Part of psql's variable hook system that ensures special variables remain visible in tab completion
- Implements the convention that bare \set commands enable boolean variables while \unset commands disable them
- Memory management is handled properly by freeing the original empty string before allocating "on"
- Used extensively during variable space establishment for various psql boolean configuration options
- The hook system ensures that special variables controlling psql behavior maintain consistent state management
- Return value is the normalized string that will be stored as the variable's value