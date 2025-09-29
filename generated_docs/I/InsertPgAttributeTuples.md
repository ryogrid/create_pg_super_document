# InsertPgAttributeTuples

## Location
[src/backend/catalog/heap.c:703-820](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/heap.c#L703-L820)

## Overview
Constructs and inserts a set of tuples in the pg_attribute system catalog, efficiently batch-inserting multiple attribute definitions for a relation.

## Definition

```c
void
InsertPgAttributeTuples(Relation pg_attribute_rel,
						TupleDesc tupdesc,
						Oid new_rel_oid,
						const FormExtraData_pg_attribute tupdesc_extra[],
						CatalogIndexState indstate)
```
## Detailed Description
InsertPgAttributeTuples is a low-level catalog management function that creates and inserts pg_attribute tuples for a set of attributes defined in a TupleDesc. The function is optimized for batch operations, creating multiple slots and inserting tuples in batches to improve performance when creating relations with many attributes.

The function copies attribute metadata from the provided TupleDesc into pg_attribute format, handling all the necessary data type conversions and field mappings. It supports both basic attribute information and extended attribute data through the tupdesc_extra parameter. The attcacheoff field is always initialized to -1, and several variable-length fields are set to null for new attributes.

The function uses a sophisticated batching mechanism that limits the number of slots based on memory constraints (MAX_CATALOG_MULTI_INSERT_BYTES) and processes attributes in groups to optimize catalog insertion performance.

## Parameters / Member Variables
- : An already opened and locked relation handle for the pg_attribute catalog
- : TupleDesc containing the attributes to insert into pg_attribute
- : Relation OID to assign to the inserted attributes; if InvalidOid, uses the relation OID from tupdesc
- : Optional array providing values for variable-length/nullable pg_attribute fields; must match tupdesc length or be NULL
- : Index state for CatalogTupleInsertWithInfo; can be NULL (will fetch necessary info automatically)

## Dependencies
- Functions called/Symbols referenced:
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md)
  - [ExecClearTuple](../E/ExecClearTuple.md)
  - [ExecStoreVirtualTuple](../E/ExecStoreVirtualTuple.md)
  - [ExecDropSingleTupleTableSlot](../E/ExecDropSingleTupleTableSlot.md)
  - [CatalogOpenIndexes](../C/CatalogOpenIndexes.md)
  - [CatalogTuplesMultiInsertWithInfo](../C/CatalogTuplesMultiInsertWithInfo.md)
  - [CatalogCloseIndexes](../C/CatalogCloseIndexes.md)
  - Various Datum conversion functions (NameGetDatum, Int16GetDatum, etc.)
- Called from (representative examples):
  - [AddNewAttributeTuples](../A/AddNewAttributeTuples.md)
  - [AppendAttributeTuples](../A/AppendAttributeTuples.md)
  - [ATExecAddColumn](../A/ATExecAddColumn.md)

## Notes and Other Information
- The function is optimized for bulk insertion operations and should be preferred over single-tuple insertion for multiple attributes
- When inserting multiple attributes, it's more efficient to pass a valid indstate parameter rather than letting the function fetch index information repeatedly
- The function automatically handles memory management for the tuple slots and ensures proper cleanup
- [Variable](../V/Variable.md)-length pg_attribute fields (attacl, attfdwoptions, attmissingval) are always set to null for new columns

## Simplified Source

```c
void
InsertPgAttributeTuples(Relation pg_attribute_rel, TupleDesc tupdesc,
                       Oid new_rel_oid,
                       const FormExtraData_pg_attribute tupdesc_extra[],
                       CatalogIndexState indstate)
{
    TupleDesc td = RelationGetDescr(pg_attribute_rel);

    // Calculate optimal batch size for memory efficiency
    int nslots = Min(tupdesc->natts,
                    (MAX_CATALOG_MULTI_INSERT_BYTES / sizeof(FormData_pg_attribute)));

    // Create array of tuple table slots for batching
    TupleTableSlot **slot = palloc(sizeof(TupleTableSlot *) * nslots);
    for (int i = 0; i < nslots; i++)
        slot[i] = MakeSingleTupleTableSlot(td, &TTSOpsHeapTuple);

    int natts = 0;
    int slotCount = 0;
    bool close_index = false;

    // Process each attribute from the tuple descriptor
    while (natts < tupdesc->natts)
    {
        Form_pg_attribute attrs = TupleDescAttr(tupdesc, natts);
        const FormExtraData_pg_attribute *attrs_extra =
            tupdesc_extra ? &tupdesc_extra[natts] : NULL;

        ExecClearTuple(slot[slotCount]);

        // Initialize all values as non-null
        memset(slot[slotCount]->tts_isnull, false,
               slot[slotCount]->tts_tupleDescriptor->natts * sizeof(bool));

        // Set core attribute values
        Oid rel_oid = (new_rel_oid != InvalidOid) ? new_rel_oid : attrs->attrelid;
        slot[slotCount]->tts_values[Anum_pg_attribute_attrelid - 1] =
            ObjectIdGetDatum(rel_oid);
        slot[slotCount]->tts_values[Anum_pg_attribute_attname - 1] =
            NameGetDatum(&attrs->attname);
        slot[slotCount]->tts_values[Anum_pg_attribute_atttypid - 1] =
            ObjectIdGetDatum(attrs->atttypid);
        slot[slotCount]->tts_values[Anum_pg_attribute_attlen - 1] =
            Int16GetDatum(attrs->attlen);
        slot[slotCount]->tts_values[Anum_pg_attribute_attnum - 1] =
            Int16GetDatum(attrs->attnum);
        slot[slotCount]->tts_values[Anum_pg_attribute_attcacheoff - 1] =
            Int32GetDatum(-1);

        // Set attribute properties
        slot[slotCount]->tts_values[Anum_pg_attribute_atttypmod - 1] =
            Int32GetDatum(attrs->atttypmod);
        slot[slotCount]->tts_values[Anum_pg_attribute_attbyval - 1] =
            BoolGetDatum(attrs->attbyval);
        slot[slotCount]->tts_values[Anum_pg_attribute_attnotnull - 1] =
            BoolGetDatum(attrs->attnotnull);
        slot[slotCount]->tts_values[Anum_pg_attribute_atthasdef - 1] =
            BoolGetDatum(attrs->atthasdef);
        slot[slotCount]->tts_values[Anum_pg_attribute_attisdropped - 1] =
            BoolGetDatum(attrs->attisdropped);
        slot[slotCount]->tts_values[Anum_pg_attribute_attislocal - 1] =
            BoolGetDatum(attrs->attislocal);
        slot[slotCount]->tts_values[Anum_pg_attribute_attinhcount - 1] =
            Int16GetDatum(attrs->attinhcount);
        slot[slotCount]->tts_values[Anum_pg_attribute_attcollation - 1] =
            ObjectIdGetDatum(attrs->attcollation);

        // Handle extended attribute data if provided
        if (attrs_extra)
        {
            slot[slotCount]->tts_values[Anum_pg_attribute_attstattarget - 1] =
                attrs_extra->attstattarget.value;
            slot[slotCount]->tts_isnull[Anum_pg_attribute_attstattarget - 1] =
                attrs_extra->attstattarget.isnull;
            slot[slotCount]->tts_values[Anum_pg_attribute_attoptions - 1] =
                attrs_extra->attoptions.value;
            slot[slotCount]->tts_isnull[Anum_pg_attribute_attoptions - 1] =
                attrs_extra->attoptions.isnull;
        }
        else
        {
            slot[slotCount]->tts_isnull[Anum_pg_attribute_attstattarget - 1] = true;
            slot[slotCount]->tts_isnull[Anum_pg_attribute_attoptions - 1] = true;
        }

        // Set remaining fields as null for new columns
        slot[slotCount]->tts_isnull[Anum_pg_attribute_attacl - 1] = true;
        slot[slotCount]->tts_isnull[Anum_pg_attribute_attfdwoptions - 1] = true;
        slot[slotCount]->tts_isnull[Anum_pg_attribute_attmissingval - 1] = true;

        ExecStoreVirtualTuple(slot[slotCount]);
        slotCount++;

        // Insert batch when slots are full or at end
        if (slotCount == nslots || natts == tupdesc->natts - 1)
        {
            if (!indstate)
            {
                indstate = CatalogOpenIndexes(pg_attribute_rel);
                close_index = true;
            }

            CatalogTuplesMultiInsertWithInfo(pg_attribute_rel, slot, slotCount, indstate);
            slotCount = 0;
        }

        natts++;
    }

    // Cleanup
    if (close_index)
        CatalogCloseIndexes(indstate);
    for (int i = 0; i < nslots; i++)
        ExecDropSingleTupleTableSlot(slot[i]);
    pfree(slot);
}
```