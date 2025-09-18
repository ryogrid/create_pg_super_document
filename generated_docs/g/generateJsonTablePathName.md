# generateJsonTablePathName

## Location
src/backend/parser/parse_jsontable.c: 231 - 250

## Overview
Generates unique system-generated path names for unnamed JSON_TABLE path specifications using a sequential naming scheme.

## Definition


## Detailed Description
This function creates unique path names for JSON_TABLE path specifications that don't have explicit names provided by the user. It uses a simple sequential numbering scheme, generating names in the format 'json_table_path_N' where N is an incrementing integer maintained in the context's pathNameId field.

The function performs several operations:
1. Creates a name using snprintf with the current pathNameId counter
2. Increments the pathNameId counter for the next call
3. Makes a permanent copy of the name using pstrdup
4. Adds the generated name to the context's pathNames list for duplicate tracking
5. Returns the generated name for use in the JSON_TABLE structure

This ensures that all path specifications have valid, unique names for internal processing, even when the user doesn't explicitly provide them.

## Parameters / Member Variables
- : JsonTableParseContext containing the pathNameId counter and pathNames list

## Dependencies
- Functions called/Symbols referenced:
  - snprintf (standard C formatted string function)
  - [pstrdup](../p/pstrdup.md) (PostgreSQL string duplication function)
  - lappend (PostgreSQL list append function)
- Called from (representative examples):
  - [transformJsonTable](../t/transformJsonTable.md) (for root path specifications)
  - [transformJsonTableNestedColumns](../t/transformJsonTableNestedColumns.md) (for nested path specifications)

## Notes and Other Information
- This is a static function, only accessible within the parse_jsontable.c module
- Uses a fixed-size buffer (32 chars) which is sufficient for the naming pattern
- The pathNameId counter is automatically incremented, ensuring uniqueness within a single JSON_TABLE
- Generated names follow a predictable pattern: json_table_path_0, json_table_path_1, etc.
- The generated name is added to pathNames list to participate in duplicate detection
- Memory for the returned string is allocated using pstrdup for proper memory management