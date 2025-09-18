# stringToNodeWithLocations

## Location
src/backend/nodes/read.c: 98 - 152

## Overview
A public API function that converts string representations of PostgreSQL Node trees back into actual Node data structures while preserving location field information.

## Definition


## Detailed Description
This function serves as an externally visible entry point for PostgreSQL's string-to-node deserialization with location field preservation. Unlike the standard stringToNode function, this variant restores location information from the string representation rather than setting location fields to -1.

The function is a wrapper around stringToNodeInternal, passing true for the restore_loc_fields parameter. This enables the parser to restore original source location information embedded in the string representation, which is particularly useful for debugging and error reporting scenarios where maintaining source position information is important.

This function is primarily used in query processing contexts where location information needs to be preserved for accurate error reporting and debugging purposes.

## Parameters / Member Variables
- : The string representation of the Node tree to be parsed with location information preserved

## Dependencies
- Functions called/Symbols referenced:
  - [stringToNodeInternal](stringToNodeInternal.md)
- Called from (representative examples):
  - [pg_parse_query](../p/pg_parse_query.md)
  - [pg_rewrite_query](../p/pg_rewrite_query.md)
  - [pg_plan_query](../p/pg_plan_query.md)

## Notes and Other Information
- Preserves location fields from the string representation instead of setting them to -1
- Primarily used in query processing contexts where source location information is valuable
- Location field restoration is only fully functional in debug builds with WRITE_READ_PARSE_PLAN_TREES enabled
- More specialized than stringToNode, with fewer callers throughout the codebase
- Essential for maintaining debugging and error reporting capabilities in query processing