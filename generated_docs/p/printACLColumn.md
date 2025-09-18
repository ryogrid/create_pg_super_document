# printACLColumn

## Location
[src/bin/psql/describe.c:6659-6676](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L6659-L6676)

## Overview
A static helper function that generates standardized SQL expressions for formatting Access Control List (ACL) privilege columns in psql describe commands.

## Definition


## Detailed Description
The  function is a utility function used throughout psql's describe.c module to consistently format ACL (Access Control List) columns in various \d commands. It generates a SQL CASE expression that handles the display of privilege information in a user-friendly format.

The function appends a SQL expression to the provided buffer that:
1. Checks if the ACL array is empty (length 0)
2. If empty, displays "(none)" to indicate no specific privileges are set
3. If not empty, converts the array to a string with newline separators for readability
4. Labels the column as "Access privileges" with proper internationalization support

This standardized approach ensures consistent privilege display across all psql describe commands that show ACL information, such as tables, functions, types, databases, schemas, etc.

## Parameters / Member Variables
- : PQExpBuffer to which the SQL expression will be appended
- : Name of the database column containing the ACL array (e.g., "relacl", "datacl", "nspacl")

## Dependencies
- Functions called/Symbols referenced:
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md) (for building the SQL expression)
  - gettext_noop (for internationalization of text strings)
- Called from (representative examples):
  - [describeTablespaces](../d/describeTablespaces.md)
  - [describeFunctions](../d/describeFunctions.md)
  - [describeTypes](../d/describeTypes.md)
  - [listAllDbs](../l/listAllDbs.md)
  - [permissionsList](permissionsList.md)
  - [listDefaultACLs](../l/listDefaultACLs.md)
  - [listLanguages](../l/listLanguages.md)
  - [listDomains](../l/listDomains.md)
  - [describeConfigurationParameters](../d/describeConfigurationParameters.md)
  - [listSchemas](../l/listSchemas.md)
  - [listForeignDataWrappers](../l/listForeignDataWrappers.md)
  - [listForeignServers](../l/listForeignServers.md)
  - [listLargeObjects](../l/listLargeObjects.md)

## Notes and Other Information
- This is a static function, only accessible within the describe.c file
- Provides consistent ACL formatting across all psql describe commands
- Uses PostgreSQL's  and  functions for array processing
- The generated SQL handles NULL ACLs by checking array length rather than NULL status
- Referenced in a comment regarding special handling needed in  for attribute ACLs
- Uses newline separators (E'\\n') to make multi-privilege entries more readable
- Part of psql's internationalization framework with gettext_noop for translatable strings
- The function only appends the SQL expression without adding decorative whitespace or commas