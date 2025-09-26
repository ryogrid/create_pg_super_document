# listOpFamilyOperators

## Location
[src/bin/psql/describe.c:6867-6964](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L6867-L6964)

## Overview
The  function implements the  psql command to display a formatted list of operators belonging to operator families, with optional filtering by index access method and operator family name.

## Definition

```c
bool
listOpFamilyOperators(const char *access_method_pattern,
					  const char *family_pattern, bool verbose)
```
## Detailed Description
This function constructs and executes an SQL query to retrieve operator information from PostgreSQL system catalogs, specifically from the  table which stores access method operators. It displays operators with their associated access methods, operator family names, operator signatures (in regoperator format), strategy numbers, and purposes (ordering vs. search). In verbose mode, it additionally shows the sort operator family information. The function supports pattern matching for filtering results by access method name and operator family name.

The query joins multiple system catalogs (, , , ) to gather comprehensive information about operators within operator families. Results are sorted by access method, operator family, operator type compatibility (self-types first), left/right operand types, and strategy number for consistent and meaningful presentation.

## Parameters / Member Variables
- : Optional regex pattern to filter results by index access method name (e.g., "btree", "hash"). If NULL, all access methods are included.
- : Optional regex pattern to filter results by operator family name. If NULL, operators from all families are included.
- : Boolean flag that controls whether to include additional columns (sort operator family) in the output.

## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - [PSQLexec](../P/PSQLexec.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - lengthof
  - [printQuery](../p/printQuery.md)
  - [PQExpBufferData](../P/PQExpBufferData.md)
  - [printQueryOpt](../p/printQueryOpt.md)
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (psql command dispatcher)

## Notes and Other Information
- This function is part of psql's describe command family, specifically handling the  command
- Pattern matching follows PostgreSQL's standard SQL pattern syntax with support for wildcards
- The function uses internationalization support through gettext_noop() for column headers
- Error handling includes proper cleanup of allocated buffers on failure paths
- The query uses visibility functions like  to handle schema-qualified names appropriately
- Operator purpose is decoded from single characters ('o' = ordering, 's' = search) to readable text
- Operators are displayed in regoperator format, which shows the operator symbol with its operand types
- The sort order prioritizes operators that work with the same left and right types (self-types) first
- Strategy numbers indicate the semantic meaning of operators within their access method (e.g., 1 = less than, 2 = less than or equal, etc.)
- In verbose mode, the sort operator family column shows the operator family used for sorting when the operator is used for ordering purposes