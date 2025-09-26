# AlterPolicyStmt

## Location
src/include/nodes/parsenodes.h: 2975 - 2983

## Overview
AlterPolicyStmt represents the parsed structure of an ALTER POLICY SQL statement, used to modify existing row-level security policies in PostgreSQL.

## Definition


## Detailed Description
AlterPolicyStmt is a parse tree node that captures the components of an ALTER POLICY statement. It allows modification of existing row-level security (RLS) policies, including changing the roles that the policy applies to, the USING expression that determines which rows are visible, and the WITH CHECK expression that determines which rows can be added or updated. The statement is parsed from SQL syntax like 'ALTER POLICY name ON table_name TO role_list USING (expression) WITH CHECK (expression)'.

## Parameters / Member Variables
- : NodeTag identifying this as an AlterPolicyStmt node
- : The name of the policy to be altered
- : RangeVar representing the table that the policy applies to
- : List of role names that the policy should apply to (can be NULL to keep existing roles)
- : Node representing the USING clause expression for row visibility (can be NULL to keep existing)
- : Node representing the WITH CHECK clause expression for row modification (can be NULL to keep existing)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for creating the node)
  - RangeVar (for table reference)
  - NodeTag (for type identification)
- Called from (representative examples):
  - AlterPolicy (in src/backend/commands/policy.c:768)
  - ProcessUtilitySlow (in src/backend/tcop/utility.c:1831)

## Notes and Other Information
- Part of PostgreSQL's row-level security (RLS) implementation
- Parsed in gram.y rule AlterPolicyStmt (line 5804)
- The structure allows for partial updates - any field can be NULL to indicate no change to that aspect
- Processed by the AlterPolicy function in src/backend/commands/policy.c
- Related to T_AlterPolicyStmt case in utility command processing