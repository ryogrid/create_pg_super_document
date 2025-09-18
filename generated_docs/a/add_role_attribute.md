# add_role_attribute

## Location
src/bin/psql/describe.c: 3749 - 3760

## Overview
A static utility function in psql that appends a role attribute string to a PQExpBuffer, handling proper comma separation for building role attribute lists.

## Definition


## Detailed Description
This simple utility function is used within the psql client to build formatted lists of role attributes when describing database roles. It manages the comma-separated formatting by checking if the buffer already contains content and adding a comma separator before appending the new attribute string. This ensures proper formatting in role descriptions displayed to users.

## Parameters / Member Variables
- : A PQExpBuffer structure that accumulates the role attribute strings
- : A constant string containing the role attribute to be added to the buffer

## Dependencies
- Functions called/Symbols referenced:
  - PQExpBuffer (PostgreSQL's expandable string buffer)
  - [appendPQExpBufferStr](appendPQExpBufferStr.md) (PostgreSQL's buffer append function)
- Called from (representative examples):
  - [describeRoles](../d/describeRoles.md) (called 7 times within this function at lines 3684, 3687, 3690, 3693, 3696, 3699, 3703)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same source file (describe.c)
- The function is specifically designed for role attribute formatting in psql's describe functionality
- It automatically handles comma separation, making it safe to call multiple times without manual separator management
- Located in src/bin/psql/describe.c:3749-3760