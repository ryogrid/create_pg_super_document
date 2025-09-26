# get_relation_policy_oid

## Location
[src/backend/commands/policy.c:1204-1255](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/policy.c#L1204-L1255)

## Overview
Looks up a policy by name on a specified relation to find its OID, with configurable error handling for missing policies.

## Definition

```c
Oid
get_relation_policy_oid(Oid relid, const char *policy_name, bool missing_ok)
```
## Detailed Description
This utility function provides a simple interface for policy OID lookup by encapsulating the catalog search process:

1. **Catalog Search**: Opens pg_policy with AccessShareLock and performs an indexed scan using both relation OID and policy name as search keys
2. **Result Processing**: Extracts the policy OID from the found tuple or handles the missing case according to the missing_ok parameter
3. **Error Handling**: Either throws a descriptive error (when missing_ok is false) or returns InvalidOid (when missing_ok is true)
4. **Resource Management**: Ensures proper cleanup of catalog scan resources and relation locks

This function serves as a building block for other policy-related operations that need to resolve policy names to OIDs.

## Parameters
- : OID of the relation (table) that owns the policy
- : Name of the policy to look up
- : Controls error handling behavior:
  - : Throw error if policy not found
  - : Return InvalidOid if policy not found

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md), table_close (catalog access)
  - [ScanKeyInit](../S/ScanKeyInit.md), systable_beginscan, systable_getnext, systable_endscan (catalog scanning)
  - HeapTupleIsValid (tuple validation)
  - [get_rel_name](get_rel_name.md) (relation name lookup for error messages)
  - Form_pg_policy, GETSTRUCT (tuple structure access)
  - ereport, errcode, errmsg (error reporting)
- Called from:
  - [get_object_address_relobject](get_object_address_relobject.md) (object address resolution for policies)

## Notes and Other Information
- Uses AccessShareLock for read-only catalog access, allowing concurrent reads
- Leverages PolicyPolrelidPolnameIndexId index for efficient lookup by (relation OID, policy name)
- Returns InvalidOid (0) when policy is not found and missing_ok is true
- Error messages include both policy name and table name for better diagnostics
- This is a utility function commonly used in DDL operations and object resolution contexts
- Policy names are unique within each table but not globally, so both relid and policy_name are required for unique identification