# describeRoleGrants

## Location
[src/bin/psql/describe.c:3830-3908](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L3830-L3908)

## Overview
A psql command function that implements the \\drg (describe role grants) metacommand to display role membership grants and their associated privileges.

## Definition

```c
bool
describeRoleGrants(const char *pattern, bool showSystem)
```
## Detailed Description
This function provides functionality for the psql \\drg metacommand, which displays role membership information including which roles are members of other roles and what privileges they have been granted. The function queries the pg_auth_members system catalog joined with pg_roles to show role relationships. It displays the member role name, the role they are a member of, options (ADMIN, INHERIT, SET privileges), and the grantor of the membership. The function adapts its query based on the PostgreSQL server version (16.0 and later have enhanced role membership options) and can filter out system roles when requested.

## Parameters / Member Variables
- : A SQL pattern (with wildcards) to filter by role name, or NULL to match all roles
- : Boolean flag indicating whether to include system roles (those starting with 'pg_') in the output

## Dependencies
- Functions called/Symbols referenced:
  - [PQExpBufferData](../P/PQExpBufferData.md) (PostgreSQL's expandable string buffer structure)
  - [printQueryOpt](../p/printQueryOpt.md) (print formatting options structure)
  - [initPQExpBuffer](../i/initPQExpBuffer.md) (initialize buffer)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md) (formatted append to buffer)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md) (validate and append SQL name patterns)
  - [termPQExpBuffer](../t/termPQExpBuffer.md) (cleanup buffer)
  - [PSQLexec](../P/PSQLexec.md) (execute SQL query)
  - [printQuery](../p/printQuery.md) (display query results)
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (psql command dispatcher at src/bin/psql/command.c:946)
  - DESCRIBE_H (function declaration in src/bin/psql/describe.h:41)

## Notes and Other Information
- Returns true on success, false on error
- Implements the psql \\drg metacommand functionality
- Version-aware: adapts query for PostgreSQL 16.0+ which introduced more granular role membership options (inherit_option, set_option)
- For versions prior to 16.0, it uses rolinherit attribute and assumes SET is always available
- By default excludes system roles (those with names starting with 'pg_') unless showSystem is true
- The query output includes role name, member of (parent role), options (ADMIN/INHERIT/SET), and grantor
- Uses LEFT JOINs to handle cases where role or grantor information might be missing
- Results are ordered by role name, parent role, and grantor for consistent display
- Located in src/bin/psql/describe.c:3830-3908