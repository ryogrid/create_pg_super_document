# EnumValuesCreate

## Location
[src/backend/catalog/pg_enum.c:84-223](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_enum.c#L84-L223)

## Overview
Creates entries in the pg_enum catalog table for each supplied enum value during CREATE TYPE AS ENUM, assigning sorted OIDs and managing transaction-level enum type tracking.

## Definition

```c
void
EnumValuesCreate(Oid enumTypeOid, List *vals)
```
## Detailed Description
EnumValuesCreate is the core function responsible for populating the pg_enum catalog with enum value entries during enum type creation. The function implements several critical PostgreSQL enum management features:

1. **Transaction Tracking**: Records the enum type OID in uncommitted_enum_types hash table if called at transaction level 1, enabling proper handling of subsequent ALTER ADD VALUE operations.

2. **OID Assignment Strategy**: Allocates even-numbered OIDs to enum values to enable direct OID comparison in enum comparison functions, avoiding the need for catalog lookups during comparisons.

3. **Batch Processing**: Uses multi-insert optimization to efficiently insert multiple enum values in batches, improving performance for enums with many values.

4. **Sort Order Management**: Assigns enumsortorder values sequentially (1, 2, 3...) to maintain proper enum value ordering.

The function assumes it will be called even for empty enum types, making it the single entry point for enum type transaction management.

## Parameters / Member Variables
- : The OID of the enum type being created
- : List of String values representing the enum labels to be created

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTransactionNestLevel](../G/GetCurrentTransactionNestLevel.md)
  - [init_uncommitted_enum_types](../i/init_uncommitted_enum_types.md)
  - [hash_search](../h/hash_search.md)
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md)
  - qsort
  - [CatalogOpenIndexes](../C/CatalogOpenIndexes.md)
  - [CatalogTuplesMultiInsertWithInfo](../C/CatalogTuplesMultiInsertWithInfo.md)
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md)
  - [ExecClearTuple](ExecClearTuple.md)
  - [ExecStoreVirtualTuple](ExecStoreVirtualTuple.md)
  - [ExecDropSingleTupleTableSlot](ExecDropSingleTupleTableSlot.md)
- Called from:
  - [DefineEnum](../D/DefineEnum.md) (src/backend/commands/typecmds.c:1221)

## Notes and Other Information
- The function deliberately does not check for duplicate values in the input list, relying on unique index violations to catch duplicates
- Even-numbered OID assignment is a performance optimization that allows enum comparison functions to compare OIDs directly without catalog lookups
- The uncommitted_enum_types tracking only occurs at transaction level 1, not in subtransactions, to optimize for the most common usage patterns
- Multi-insert batching is limited by MAX_CATALOG_MULTI_INSERT_BYTES to control memory usage
- Enum labels are stored in NAME fields and are subject to NAMEDATALEN length restrictions

## Simplified Source

```c
void
EnumValuesCreate(Oid enumTypeOid, List *vals)
{
    Relation pg_enum;
    Oid *oids;
    int elemno, num_elems;
    ListCell *lc;
    int slotCount = 0, nslots;
    CatalogIndexState indstate;
    TupleTableSlot **slot;

    // Record enum type in uncommitted_enum_types hash if at top level
    if (GetCurrentTransactionNestLevel() == 1) {
        if (uncommitted_enum_types == NULL)
            init_uncommitted_enum_types();
        hash_search(uncommitted_enum_types, &enumTypeOid, HASH_ENTER, NULL);
    }

    num_elems = list_length(vals);
    pg_enum = table_open(EnumRelationId, RowExclusiveLock);

    // Allocate even-numbered OIDs for proper sort order
    oids = (Oid *) palloc(num_elems * sizeof(Oid));
    for (elemno = 0; elemno < num_elems; elemno++) {
        Oid new_oid;
        do {
            new_oid = GetNewOidWithIndex(pg_enum, EnumOidIndexId, Anum_pg_enum_oid);
        } while (new_oid & 1);  // Ensure even OID
        oids[elemno] = new_oid;
    }

    // Sort OIDs in case counter wrapped
    qsort(oids, num_elems, sizeof(Oid), oid_cmp);

    // Set up for batch insertion
    indstate = CatalogOpenIndexes(pg_enum);
    nslots = Min(num_elems, MAX_CATALOG_MULTI_INSERT_BYTES / sizeof(FormData_pg_enum));
    slot = palloc(sizeof(TupleTableSlot *) * nslots);
    for (int i = 0; i < nslots; i++)
        slot[i] = MakeSingleTupleTableSlot(RelationGetDescr(pg_enum), &TTSOpsHeapTuple);

    // Insert enum values
    elemno = 0;
    foreach(lc, vals) {
        char *lab = strVal(lfirst(lc));
        Name enumlabel = palloc0(NAMEDATALEN);

        // Validate label length
        if (strlen(lab) > (NAMEDATALEN - 1))
            ereport(ERROR, "invalid enum label length");

        // Prepare tuple slot
        ExecClearTuple(slot[slotCount]);
        memset(slot[slotCount]->tts_isnull, false,
               slot[slotCount]->tts_tupleDescriptor->natts * sizeof(bool));

        // Set tuple values
        slot[slotCount]->tts_values[Anum_pg_enum_oid - 1] = ObjectIdGetDatum(oids[elemno]);
        slot[slotCount]->tts_values[Anum_pg_enum_enumtypid - 1] = ObjectIdGetDatum(enumTypeOid);
        slot[slotCount]->tts_values[Anum_pg_enum_enumsortorder - 1] = Float4GetDatum(elemno + 1);
        namestrcpy(enumlabel, lab);
        slot[slotCount]->tts_values[Anum_pg_enum_enumlabel - 1] = NameGetDatum(enumlabel);

        ExecStoreVirtualTuple(slot[slotCount]);
        slotCount++;

        // Insert batch when slots are full
        if (slotCount == nslots) {
            CatalogTuplesMultiInsertWithInfo(pg_enum, slot, slotCount, indstate);
            slotCount = 0;
        }
        elemno++;
    }

    // Insert remaining tuples
    if (slotCount > 0)
        CatalogTuplesMultiInsertWithInfo(pg_enum, slot, slotCount, indstate);

    // Cleanup
    pfree(oids);
    for (int i = 0; i < nslots; i++)
        ExecDropSingleTupleTableSlot(slot[i]);
    CatalogCloseIndexes(indstate);
    table_close(pg_enum, RowExclusiveLock);
}
```