# CreatePolicyStmt

## Location
src/include/nodes/parsenodes.h: 2959 - 2969

## Overview
CreatePolicyStmt is a parse node structure that represents a CREATE POLICY SQL statement, which creates a row-level security policy that controls access to rows in a table based on specified conditions and roles.

## Definition
```c
typedef struct CreatePolicyStmt
{
    NodeTag     type;
    char       *policy_name;    /* Policy's name */
    RangeVar   *table;          /* the table name the policy applies to */
    char       *cmd_name;       /* the command name the policy applies to */
    bool        permissive;     /* restrictive or permissive policy */
    List       *roles;          /* the roles associated with the policy */
    Node       *qual;           /* the policy's condition */
    Node       *with_check;     /* the policy's WITH CHECK condition. */
} CreatePolicyStmt;
```

## Detailed Description
CreatePolicyStmt is a parse tree node that stores the parsed representation of a CREATE POLICY statement. This structure contains all the information needed to create a row-level security (RLS) policy on a table. RLS policies define conditions that determine which rows are visible to or modifiable by different users or roles, providing fine-grained access control at the row level within PostgreSQL tables.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a CreatePolicyStmt node
- `policy_name`: String containing the name of the policy to create
- `table`: RangeVar pointer specifying the table to which the policy applies
- `cmd_name`: String specifying the SQL command type the policy applies to (ALL, SELECT, INSERT, UPDATE, DELETE)
- `permissive`: Boolean indicating whether this is a permissive policy (true) or restrictive policy (false)
- `roles`: List of role names (RoleSpec nodes) that the policy applies to; NULL means it applies to all roles
- `qual`: Node containing the policy's USING expression that determines row visibility
- `with_check`: Node containing the policy's WITH CHECK expression for INSERT/UPDATE operations

## Dependencies
- Functions called/Symbols referenced:
  - RangeVar
  - NodeTag
  - List
  - Node
- Called from (representative examples):
  - CreatePolicy (src/backend/commands/policy.c:569)
  - ProcessUtilitySlow (src/backend/tcop/utility.c:1827)

## Notes and Other Information
- This structure is part of the PostgreSQL parser node hierarchy and inherits from the Node structure via NodeTag
- Row-level security must be enabled on the table (ALTER TABLE ... ENABLE ROW LEVEL SECURITY) for policies to take effect
- Permissive policies are ORed together while restrictive policies are ANDed together
- The qual expression is evaluated for row visibility, while with_check is evaluated for row modification operations
- Policies are stored in the pg_policy system catalog
- The cmd_name can specify which DML operations the policy applies to, or ALL for all operations
- This is defined in src/include/nodes/parsenodes.h:2959-2969