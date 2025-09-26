# insert_tobeinserted

## Location
src/interfaces/ecpg/ecpglib/execute.c: 1127 - 1158

## Overview
A static helper function that replaces a placeholder in an SQL command string with the actual parameter value or expression.

## Definition


## Detailed Description
This function performs string manipulation to replace placeholders in SQL commands with actual values during ECPG parameter processing. It creates a new command string by concatenating three parts: the command up to the placeholder position, the replacement text, and the rest of the original command after the placeholder. The function handles memory allocation for the new string and ensures proper cleanup of both the old command and the replacement text.

## Parameters / Member Variables
- : The 1-based position in the command string where replacement should occur
- : The length of the placeholder being replaced
- : Pointer to the statement structure containing the command to modify
- : The replacement text to insert at the specified position

## Dependencies
- Functions called/Symbols referenced:
  - ecpg_alloc
  - ecpg_free
- Called from:
  - ecpg_build_params (multiple locations)

## Notes and Other Information
- Returns true on successful replacement, false on memory allocation failure
- The function automatically frees the tobeinserted parameter regardless of success/failure
- Uses 1-based positioning for the replacement location
- Allocates a new command string and frees the old one, updating the statement structure
- Critical for parameter substitution in prepared SQL statements within ECPG
- Memory management includes cleanup of both input parameters and intermediate allocations