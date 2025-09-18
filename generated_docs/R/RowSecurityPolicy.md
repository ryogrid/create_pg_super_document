# RowSecurityPolicy

## Location
src/include/rewrite/rowsecurity.h: 20 - 29

## Overview
RowSecurityPolicy represents a single row-level security policy in PostgreSQL, containing all the metadata and expressions needed to enforce access control at the row level for database tables.

## Definition


## Detailed Description
RowSecurityPolicy is a core data structure in PostgreSQL's Row Level Security (RLS) implementation. Each instance represents a single policy that controls which rows a user can see or modify in a table. The structure contains both metadata about the policy (name, command type, applicable roles) and the actual security expressions that are evaluated during query execution.

The policy can be either permissive (allowing access to rows that match the condition) or restrictive (denying access to rows that match the condition). The structure supports different expressions for read operations (qual) and write operations (with_check_qual), allowing fine-grained control over data access and modification.

## Parameters / Member Variables
- : String containing the user-defined name of the row security policy
- : Character indicating the SQL command type this policy applies to (SELECT, INSERT, UPDATE, DELETE, or ALL)
- : Array of role OIDs that this policy applies to; if NULL, applies to all roles
- : Boolean flag indicating policy type - true for permissive policies (grant access), false for restrictive policies (deny access)
- : Expression tree used to filter which rows are visible/accessible for read operations
- : Expression tree used to validate rows for write operations (INSERT/UPDATE)
- : Boolean optimization flag indicating whether either qual or with_check_qual contains subqueries

## Dependencies
- Functions called/Symbols referenced:
  - ArrayType (for roles array)
  - Expr (for qualification expressions)
  - MemoryContext (implicitly through expression trees)

- Called from (representative examples):
  - RelationBuildRowSecurity (policy.c:237, 242)
  - get_policies_for_relation (rowsecurity.c:554, 634, 648)
  - add_security_quals (rowsecurity.c:716, 742)
  - row_security_policy_cmp (rowsecurity.c:676, 677)
  - equalPolicy (relcache.c:953)

## Notes and Other Information
- Policies are stored as part of the relation cache and are built when a relation is accessed
- The hassublinks flag is used for optimization to avoid unnecessary subquery processing
- Permissive and restrictive policies can be combined - all restrictive policies must pass AND at least one permissive policy must pass
- The polcmd field uses single character codes: 'r' for SELECT, 'a' for INSERT, 'w' for UPDATE, 'd' for DELETE, '*' for ALL
- Expressions are stored in parsed form and are re-evaluated for each query execution
- Memory management for policies is handled through the relation's row security memory context