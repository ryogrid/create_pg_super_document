# RelationBuildTriggers

## Location
[src/backend/commands/trigger.c:1856-2007](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L1856-L2007)

## Overview
RelationBuildTriggers builds trigger data to attach to the given relcache entry by scanning the pg_trigger system catalog and constructing a complete TriggerDesc structure for efficient trigger processing.

## Definition
void RelationBuildTriggers(Relation relation)

## Detailed Description
This function constructs trigger metadata for a given relation by performing the following operations:

1. **Memory Management Strategy**: Creates a temporary TriggerDesc structure in working memory context to avoid cache memory leaks if the operation fails partway through, then copies the completed structure to CacheMemoryContext for long-term storage.

2. **Catalog Scanning**: Scans the pg_trigger system catalog using TriggerRelidNameIndexId to find all triggers associated with the relation. The scan is performed in name order, ensuring triggers will be fired in alphabetical order.

3. **Trigger Structure Building**: For each trigger found, constructs a complete Trigger structure including:
   - Basic properties (OID, name, function OID, type, enabled status)
   - Constraint information (constraint relation, index, deferrable settings)
   - Attribute arrays for column-specific triggers
   - Argument arrays for trigger function parameters
   - Table transition names (OLD TABLE, NEW TABLE) for statement-level triggers
   - WHEN clause qualification expressions

4. **Dynamic Array Management**: Uses a dynamically resizable array starting with 16 slots that doubles in size when more triggers are found.

5. **Flag Setting**: Calls SetTriggerFlags() for each trigger to set appropriate trigger type flags in the TriggerDesc structure.

6. **Cache Integration**: Copies the completed trigger descriptor to cache memory and releases working memory.

## Parameters / Member Variables
- : The Relation structure for which to build trigger information

## Dependencies
- Functions called/Symbols referenced:
  - [SetTriggerFlags](../S/SetTriggerFlags.md): Sets trigger type flags in TriggerDesc
  - [CopyTriggerDesc](../C/CopyTriggerDesc.md): Copies TriggerDesc to cache memory context
  - [FreeTriggerDesc](../F/FreeTriggerDesc.md): Releases working memory for TriggerDesc
  - [systable_beginscan](../s/systable_beginscan.md)/systable_getnext: System catalog scanning
  - [fastgetattr](../f/fastgetattr.md): Extracts attributes from HeapTuple
  - DirectFunctionCall1/nameout: Name conversion utilities
  - [DatumGetCString](../D/DatumGetCString.md)/TextDatumGetCString: Datum conversion utilities

- Called from (representative examples):
  - [RelationBuildDesc](RelationBuildDesc.md): During relation cache entry construction

## Notes and Other Information
- The function ensures triggers are processed in name order by using TriggerRelidNameIndexId
- Memory is carefully managed to prevent leaks in the cache context
- Handles variable-length fields like trigger arguments and attribute lists
- Returns early if no triggers are found for the relation
- The resulting TriggerDesc is stored in the relation's cache entry for efficient trigger execution
- Uses CacheMemoryContext for the final trigger descriptor to ensure it survives as long as the relcache entry

## Simplified Source

```c
void RelationBuildTriggers(Relation relation)
{
    TriggerDesc *trigdesc;
    int numtrigs = 0;
    int maxtrigs = 16;
    Trigger *triggers;
    Relation tgrel;
    ScanKeyData skey;
    SysScanDesc tgscan;
    HeapTuple htup;
    MemoryContext oldContext;
    int i;

    // Allocate working array for triggers (will expand if needed)
    triggers = (Trigger *) palloc(maxtrigs * sizeof(Trigger));

    // Set up scan key for this relation's triggers
    ScanKeyInit(&skey, Anum_pg_trigger_tgrelid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(RelationGetRelid(relation)));

    // Scan pg_trigger catalog in name order
    tgrel = table_open(TriggerRelationId, AccessShareLock);
    tgscan = systable_beginscan(tgrel, TriggerRelidNameIndexId, true,
                               NULL, 1, &skey);

    // Build trigger structures from catalog entries
    while (HeapTupleIsValid(htup = systable_getnext(tgscan))) {
        Form_pg_trigger pg_trigger = (Form_pg_trigger) GETSTRUCT(htup);
        Trigger *build;
        Datum datum;
        bool isnull;

        // Expand array if needed
        if (numtrigs >= maxtrigs) {
            maxtrigs *= 2;
            triggers = (Trigger *) repalloc(triggers, maxtrigs * sizeof(Trigger));
        }
        build = &(triggers[numtrigs]);

        // Copy basic trigger properties
        build->tgoid = pg_trigger->oid;
        build->tgname = DatumGetCString(DirectFunctionCall1(nameout,
                                                           NameGetDatum(&pg_trigger->tgname)));
        build->tgfoid = pg_trigger->tgfoid;
        build->tgtype = pg_trigger->tgtype;
        build->tgenabled = pg_trigger->tgenabled;
        build->tgisinternal = pg_trigger->tgisinternal;
        build->tgisclone = OidIsValid(pg_trigger->tgparentid);

        // Copy constraint information
        build->tgconstrrelid = pg_trigger->tgconstrrelid;
        build->tgconstrindid = pg_trigger->tgconstrindid;
        build->tgconstraint = pg_trigger->tgconstraint;
        build->tgdeferrable = pg_trigger->tgdeferrable;
        build->tginitdeferred = pg_trigger->tginitdeferred;
        build->tgnargs = pg_trigger->tgnargs;

        // Copy attribute array if present
        build->tgnattr = pg_trigger->tgattr.dim1;
        if (build->tgnattr > 0) {
            build->tgattr = (int16 *) palloc(build->tgnattr * sizeof(int16));
            memcpy(build->tgattr, &(pg_trigger->tgattr.values),
                   build->tgnattr * sizeof(int16));
        } else {
            build->tgattr = NULL;
        }

        // Parse trigger arguments if present
        if (build->tgnargs > 0) {
            bytea *val = DatumGetByteaPP(fastgetattr(htup, Anum_pg_trigger_tgargs,
                                                   tgrel->rd_att, &isnull));
            if (isnull)
                elog(ERROR, "tgargs is null in trigger for relation \"%s\"",
                     RelationGetRelationName(relation));

            char *p = (char *) VARDATA_ANY(val);
            build->tgargs = (char **) palloc(build->tgnargs * sizeof(char *));
            for (i = 0; i < build->tgnargs; i++) {
                build->tgargs[i] = pstrdup(p);
                p += strlen(p) + 1;
            }
        } else {
            build->tgargs = NULL;
        }

        // Extract table transition names and WHEN clause
        datum = fastgetattr(htup, Anum_pg_trigger_tgoldtable, tgrel->rd_att, &isnull);
        build->tgoldtable = isnull ? NULL : DatumGetCString(DirectFunctionCall1(nameout, datum));

        datum = fastgetattr(htup, Anum_pg_trigger_tgnewtable, tgrel->rd_att, &isnull);
        build->tgnewtable = isnull ? NULL : DatumGetCString(DirectFunctionCall1(nameout, datum));

        datum = fastgetattr(htup, Anum_pg_trigger_tgqual, tgrel->rd_att, &isnull);
        build->tgqual = isnull ? NULL : TextDatumGetCString(datum);

        numtrigs++;
    }

    systable_endscan(tgscan);
    table_close(tgrel, AccessShareLock);

    // Return early if no triggers found
    if (numtrigs == 0) {
        pfree(triggers);
        return;
    }

    // Build trigger descriptor and set flags
    trigdesc = (TriggerDesc *) palloc0(sizeof(TriggerDesc));
    trigdesc->triggers = triggers;
    trigdesc->numtriggers = numtrigs;
    for (i = 0; i < numtrigs; i++)
        SetTriggerFlags(trigdesc, &(triggers[i]));

    // Copy to cache memory and clean up working memory
    oldContext = MemoryContextSwitchTo(CacheMemoryContext);
    relation->trigdesc = CopyTriggerDesc(trigdesc);
    MemoryContextSwitchTo(oldContext);
    FreeTriggerDesc(trigdesc);
}
```