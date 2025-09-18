# tstoken_list_member

## Location
src/backend/commands/tsearchcmds.c: 1204 - 1228

## Overview
A static utility function that checks whether a given token type name exists as a member of a TSTokenTypeItem list.

## Definition


## Detailed Description
This function performs a linear search through a list of TSTokenTypeItem structures to determine if a specified token type name is present. It iterates through each list element, comparing the provided token name with the name field of each TSTokenTypeItem using string comparison. The function returns true immediately upon finding a match, providing an early exit optimization.

## Parameters / Member Variables
- : A null-terminated string representing the token type name to search for
- : A PostgreSQL List containing TSTokenTypeItem structures to search through

## Dependencies
- Functions called/Symbols referenced:
  - TSTokenTypeItem (structure type)
  - lfirst (PostgreSQL list macro)
  - strcmp (standard C string comparison)
  - foreach (PostgreSQL list iteration macro)
- Called from (representative examples):
  - [getTokenTypes](../g/getTokenTypes.md)

## Notes and Other Information
- This is a static function, only accessible within the tsearchcmds.c file
- Uses PostgreSQL's List data structure and associated macros for iteration
- Performs case-sensitive string matching using strcmp
- Returns false if the token name is not found in the list or if the list is empty
- The function is part of PostgreSQL's text search configuration management functionality