# OperatorIsVisible

## Location
[src/backend/catalog/namespace.c:2049-2060](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L2049-L2060)

## Overview
OperatorIsVisible determines whether an operator identified by its OID is visible in the current search path.

## Definition


## Detailed Description
This function serves as a simple wrapper around OperatorIsVisibleExt to check operator visibility. An operator is considered "visible" if it would be found when searching for the unqualified operator name with exact argument matches in the current namespace search path. This function is essential for determining whether an operator can be referenced without schema qualification in SQL statements.

## Parameters / Member Variables
- : OID of the operator to check for visibility

## Dependencies
- Functions called/Symbols referenced:
  - [OperatorIsVisibleExt](OperatorIsVisibleExt.md)
- Called from (representative examples):
  - [format_operator_extended](../f/format_operator_extended.md)

## Notes and Other Information
- Returns true if the operator is visible in the current search path, false otherwise
- This is a convenience wrapper that passes NULL as the second parameter to OperatorIsVisibleExt
- Used primarily in system functions that format operator information for display
- Critical for PostgreSQL's schema-based namespace management system