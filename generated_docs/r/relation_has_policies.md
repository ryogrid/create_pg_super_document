# relation_has_policies

## Location
[src/backend/commands/policy.c:1256-1279](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/policy.c#L1256-L1279)

## Overview
Determines whether a given relation has any row-level security policies defined, providing a quick check for policy existence without retrieving the actual policy details.

## Definition

```c
bool
relation_has_policies(Relation rel)
```
## Detailed Description
The `relation_has_policies` function performs a system catalog scan to check if any row-level security policies are defined for a specific relation. It queries the `pg_policy` catalog table to determine policy existence. The function is designed as an efficient boolean check that avoids the overhead of loading policy details when only existence verification is needed.

The function opens the policy catalog table (`pg_policy`) with an `AccessShareLock`, performs a system scan using the relation's OID as the search key, and returns `true` if at least one policy tuple is found, `false` otherwise. This approach ensures consistent and safe access to the system catalog while minimizing lock contention.

## Parameters / Member Variables
- `rel`: A `Relation` structure representing the table or view to check for policies. The relation must be a valid, opened relation object.

## Dependencies
- Functions called/Symbols referenced:
  - `[table_open](../t/table_open.md)`: Opens the pg_policy catalog table
  - [ScanKeyInit](../S/ScanKeyInit.md): Initializes the scan key for catalog search
  - [systable_beginscan](../s/systable_beginscan.md): Begins the system catalog scan
  - [systable_getnext](../s/systable_getnext.md): Retrieves the next tuple from the scan
  - `HeapTupleIsValid`: Checks if the retrieved tuple is valid
  - [systable_endscan](../s/systable_endscan.md): Ends the system catalog scan
  - `[table_close](../t/table_close.md)`: Closes the catalog table
  - `RelationGetRelid`: Gets the OID of the relation
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md): Converts OID to Datum format
- Called from (representative examples):
  - Declared in `src/include/commands/policy.h`

## Notes and Other Information
- The function uses `AccessShareLock` to ensure safe concurrent access to the policy catalog
- It performs a single tuple check - if any policy exists, it immediately returns `true` without scanning further
- The scan uses the `PolicyPolrelidPolnameIndexId` index for efficient lookup by relation OID
- This is a utility function primarily used for quick policy existence checks in the row-level security subsystem
- The function is thread-safe and follows PostgreSQL's standard catalog access patterns
- Returns immediately upon finding the first policy, making it efficient for relations with many policies

## Simplified Source

```c
bool
relation_has_policies(Relation rel)
{
    Relation catalog;
    ScanKeyData skey;
    SysScanDesc sscan;
    HeapTuple policy_tuple;
    bool ret = false;

    // Open pg_policy catalog and search for policies on this relation
    catalog = table_open(PolicyRelationId, AccessShareLock);
    ScanKeyInit(&skey, Anum_pg_policy_polrelid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(RelationGetRelid(rel)));

    sscan = systable_beginscan(catalog, PolicyPolrelidPolnameIndexId, true, NULL, 1, &skey);
    policy_tuple = systable_getnext(sscan);

    // Return true if any policy found
    if (HeapTupleIsValid(policy_tuple))
        ret = true;

    systable_endscan(sscan);
    table_close(catalog, AccessShareLock);

    return ret;
}
```