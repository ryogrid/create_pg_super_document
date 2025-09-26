# is_member_of_role

## Location
[src/backend/utils/adt/acl.c:5231-5258](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L5231-L5258)

## Overview
Determines whether a given user/role is a member of another role, either directly or indirectly through role inheritance chains.

## Definition

```c
bool
is_member_of_role(Oid member, Oid role)
```
## Detailed Description
This function checks if a member (user or role) is a member of a target role through PostgreSQL's role membership system. The function performs recursive checking through the role inheritance hierarchy, following both inherited and non-inherited grants. 

**Important Usage Warning**: The source code explicitly warns against using this function for most common scenarios:
- Do NOT use for privilege checking (use  instead)  
- Do NOT use for determining SET ROLE permissions (use  instead)
- Do NOT use for object ownership validation (use  instead)

The function implements fast-path optimizations for simple cases and leverages PostgreSQL's superuser privileges where applicable.

## Parameters / Member Variables
- : The OID of the user/role being tested for membership
- : The OID of the target role to check membership against

## Dependencies
- Functions called/Symbols referenced:
  - : Checks if the member has superuser privileges
  - : Searches for the target role in a list of roles
  - : Recursively finds all roles that member belongs to
  - : Constant controlling recursion behavior
- Called from (representative examples):
  - : Access control checking functionality
  - Various ACL-related functions for role validation

## Notes and Other Information
- Returns  immediately if member equals role (identity check)
- Superusers are considered members of every role in the system
- The function recursively traverses the entire role membership hierarchy
- Despite its apparent utility, the extensive warnings in the source code indicate this function has very limited appropriate use cases in PostgreSQL's security model
- Located in 