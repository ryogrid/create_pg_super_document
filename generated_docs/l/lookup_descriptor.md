# lookup_descriptor

## Location
src/interfaces/ecpg/preproc/descriptor.c: 131 - 161

## Overview
Searches for an existing SQL descriptor by name and optionally by connection, returning a pointer to the descriptor structure if found.

## Definition


## Detailed Description
This function searches through the global linked list of descriptors to find a descriptor with the specified name. It handles connection-specific lookups and can automatically bind a descriptor to a connection if the descriptor exists without a connection and a connection is specified. The function validates that the descriptor name starts with a double quote character before performing the search.

The function implements the following logic:
- Returns NULL immediately if the name doesn't start with a double quote
- Iterates through the global descriptors linked list
- Matches descriptors by name using string comparison
- For connection handling:
  - If no connection is specified, matches descriptors without connections
  - If a connection is specified, matches descriptors with the same connection
  - If a connection is specified but the descriptor has no connection, it binds the descriptor to the connection
- Issues warning messages for non-existent descriptors

## Parameters / Member Variables
- : The name of the descriptor to look up (must start with double quote)
- : Optional connection name to associate with the descriptor (can be NULL for default connection)

## Dependencies
- Functions called/Symbols referenced:
  - struct descriptor (descriptor structure type)
  - [mm_strdup](../m/mm_strdup.md) (memory management string duplication)
  - mmerror (error reporting with PARSE_ERROR and ET_WARNING)
- Called from (representative examples):
  - No direct callers found in current analysis

## Notes and Other Information
- The function modifies the global descriptors linked list by potentially adding connection information to existing descriptors
- Warning messages are issued when descriptors are not found, distinguishing between default and named connections
- The requirement for names to start with double quotes suggests this is part of SQL parsing where quoted identifiers are expected
- This is part of the ECPG (Embedded SQL in C) preprocessor functionality for handling SQL descriptors