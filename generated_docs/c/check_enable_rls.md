# check_enable_rls

## Location
[src/backend/utils/misc/rls.c:52-141](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/rls.c#L52-L141)

## Overview
Determines whether Row Level Security (RLS) should be applied to a query based on the relation, row_security setting, and current role.

## Definition


## Detailed Description
This function evaluates whether Row Level Security (RLS) should be enabled for a given relation in the current query context. It returns one of three values:
- : RLS is not applicable to the relation at all
- : RLS is not applied for this query, but environmental changes may affect this decision  
- : RLS should be implemented and the plan cache needs invalidation if the environment changes

The function considers several factors:
1. Built-in relations (with OID < FirstNormalObjectId) never have RLS
2. Relations without  flag don't use RLS
3. Users with BYPASSRLS privilege (including superusers) bypass RLS
4. Table owners generally bypass RLS unless FORCE ROW LEVEL SECURITY is set
5. The  GUC setting can force an error instead of applying RLS

## Parameters / Member Variables
- : OID of the relation to check RLS status for
- : OID of user to check permissions as (use InvalidOid for current user)
- : If true, returns RLS_ENABLED instead of throwing error when user attempts unauthorized RLS bypass

## Dependencies
- Functions called/Symbols referenced:
  - [has_bypassrls_privilege](../h/has_bypassrls_privilege.md)
  - [object_ownercheck](../o/object_ownercheck.md)
  - [InNoForceRLSOperation](../I/InNoForceRLSOperation.md)
  - [get_rel_name](../g/get_rel_name.md)
  - Form_pg_class
  - FirstNormalObjectId
- Called from (representative examples):
  - [row_security_active](../r/row_security_active.md)
  - [row_security_active_name](../r/row_security_active_name.md)
  - [get_row_security_policies](../g/get_row_security_policies.md)
  - [DoCopy](../D/DoCopy.md)
  - [ExecBuildSlotValueDescription](../E/ExecBuildSlotValueDescription.md)

## Notes and Other Information
- The function handles checking permissions as another role via checkAsUser parameter, useful for views and security definer functions
- The noError parameter allows callers to test RLS status without triggering errors, useful in error handling contexts
- The RLS_NONE_ENV return value indicates environment-dependent decisions that may affect plan caching
- Special handling exists for referential integrity checks through InNoForceRLSOperation context