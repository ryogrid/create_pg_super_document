# AccessPriv

## Location
src/include/nodes/parsenodes.h: 2540 - 2545

## Overview
AccessPriv is a parse tree node structure that represents an access privilege specification with an optional list of column names, used in SQL privilege grant/revoke statements.

## Definition


## Detailed Description
AccessPriv represents an individual access privilege in SQL GRANT and REVOKE statements. It encapsulates both the privilege name (such as "SELECT", "INSERT", "UPDATE", etc.) and an optional list of column names to which the privilege applies. The structure supports fine-grained privilege specification at the column level.

Special handling is provided for the ALL PRIVILEGES case:
- When priv_name is NULL with a non-empty column list, it denotes ALL PRIVILEGES for those specific columns
- When cols is NIL (empty), it denotes "all columns" for the specified privilege
- Simple "ALL PRIVILEGES" without column specification is represented as a NIL list, not as an AccessPriv with both fields null

## Parameters / Member Variables
- : NodeTag identifying this as an AccessPriv node in the parse tree
- : String name of the specific privilege (e.g., "SELECT", "INSERT", "UPDATE", "DELETE", etc.). NULL indicates ALL PRIVILEGES when used with a column list
- : List of String nodes representing column names. NIL (empty list) indicates the privilege applies to all columns

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (for parse tree node identification)
  - List (PostgreSQL list data structure)

- Called from (representative examples):
  - ExecuteGrantStmt
  - ExecAlterDefaultPrivilegesStmt
  - ExecGrant_Relation
  - GrantRole

## Notes and Other Information
- This structure is part of the SQL parser's abstract syntax tree (AST) representation
- The design allows for both table-level and column-level privilege specifications
- The special encoding for ALL PRIVILEGES helps distinguish between different forms of privilege grants
- Column names are stored as String nodes in the cols list for consistency with other parse tree structures