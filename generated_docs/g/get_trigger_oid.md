# get_trigger_oid

## Location
[src/backend/commands/trigger.c:1366-1415](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L1366-L1415)

## Overview
get_trigger_oid is a utility function that looks up a trigger by name and relation ID to return its OID, with optional error handling for missing triggers.

## Definition
```c
Oid get_trigger_oid(Oid relid, const char *trigname, bool missing_ok)
```

## Detailed Description
get_trigger_oid provides a convenient interface for finding triggers by name within a specific relation. The function searches the pg_trigger system catalog using both the relation OID and trigger name to locate the unique trigger entry. It supports two modes of operation: strict mode where missing triggers cause an error, and permissive mode where missing triggers simply return InvalidOid. This dual behavior makes it suitable for both validation scenarios where the trigger must exist and exploratory scenarios where the caller needs to check for trigger existence.

## Parameters / Member Variables
- `relid`: OID of the relation that owns the trigger
- `trigname`: Name of the trigger to look up
- `missing_ok`: If false, throw error when trigger not found; if true, return InvalidOid

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)/systable_getnext
  - [get_rel_name](get_rel_name.md)
  - Form_pg_trigger
  - [CStringGetDatum](../C/CStringGetDatum.md)
- Called from (representative examples):
  - [get_object_address_relobject](get_object_address_relobject.md)

## Notes and Other Information
- Uses composite index TriggerRelidNameIndexId for efficient lookup by (relation, name)
- Takes AccessShareLock on pg_trigger for read consistency
- Returns the actual trigger OID from the pg_trigger.oid field
- Provides helpful error message including both trigger and table names when not found
- Part of PostgreSQL's object address resolution system for trigger objects
- Used primarily for resolving trigger references in DDL operations and system queries
- The missing_ok parameter follows PostgreSQL's common pattern for optional error handling

## Simplified Source

```c
Oid
get_trigger_oid(Oid relid, const char *trigname, bool missing_ok)
{
    Relation tgrel;
    ScanKeyData skey[2];
    SysScanDesc tgscan;
    HeapTuple tup;
    Oid oid;

    // Open pg_trigger catalog
    tgrel = table_open(TriggerRelationId, AccessShareLock);

    // Setup scan keys for relation ID and trigger name
    ScanKeyInit(&skey[0], Anum_pg_trigger_tgrelid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(relid));
    ScanKeyInit(&skey[1], Anum_pg_trigger_tgname,
                BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(trigname));

    // Begin indexed scan
    tgscan = systable_beginscan(tgrel, TriggerRelidNameIndexId, true,
                               NULL, 2, skey);

    tup = systable_getnext(tgscan);

    if (!HeapTupleIsValid(tup))
    {
        if (!missing_ok)
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("trigger \"%s\" for table \"%s\" does not exist",
                            trigname, get_rel_name(relid))));
        oid = InvalidOid;
    }
    else
    {
        oid = ((Form_pg_trigger) GETSTRUCT(tup))->oid;
    }

    systable_endscan(tgscan);
    table_close(tgrel, AccessShareLock);
    return oid;
}
```