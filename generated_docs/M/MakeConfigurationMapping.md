# MakeConfigurationMapping

## Location
[src/backend/commands/tsearchcmds.c:1288-1490](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tsearchcmds.c#L1288-L1490)

## Overview
A static function that implements the core logic for ALTER TEXT SEARCH CONFIGURATION ADD/ALTER MAPPING commands, managing token-to-dictionary mappings in PostgreSQL's text search system.

## Definition

```c
static void
MakeConfigurationMapping(AlterTSConfigurationStmt *stmt,
						 HeapTuple tup, Relation relMap)
```
## Detailed Description
This function handles the complex process of adding or modifying token-to-dictionary mappings for text search configurations. It supports three main operations: 1) Adding new mappings (default mode), 2) Replacing specific dictionaries in existing mappings (replace mode), and 3) Overriding existing mappings for specified token types (override mode). The function first validates token types using getTokenTypes, then processes dictionary names to get their OIDs. For override operations, it deletes existing mappings for the specified tokens. For replace operations, it scans existing mappings and updates dictionary references. For new mappings, it uses batch insertion with TupleTableSlots for optimal performance, inserting multiple tuples per batch operation.

## Parameters / Member Variables
- `*stmt`: AlterTSConfigurationStmt structure containing the SQL command details including token types, dictionaries, and operation flags (override, replace)
- `tup`: HeapTuple representing the text search configuration record from pg_ts_config
- `relMap`: Relation handle for the pg_ts_config_map catalog table where mappings are stored
## Dependencies
- Functions called/Symbols referenced:
  - [AlterTSConfigurationStmt](../A/AlterTSConfigurationStmt.md)
  - Form_pg_ts_config
  - [getTokenTypes](../g/getTokenTypes.md)
  - TSTokenTypeItem
  - [systable_beginscan](../s/systable_beginscan.md), systable_getnext, systable_endscan
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md), CatalogTupleUpdateWithInfo
  - [get_ts_dict_oid](../g/get_ts_dict_oid.md)
  - [CatalogOpenIndexes](../C/CatalogOpenIndexes.md), CatalogCloseIndexes
  - [MakeSingleTupleTableSlot](MakeSingleTupleTableSlot.md), ExecDropSingleTupleTableSlot
  - [ExecClearTuple](../E/ExecClearTuple.md), ExecStoreVirtualTuple
  - [CatalogTuplesMultiInsertWithInfo](../C/CatalogTuplesMultiInsertWithInfo.md)
  - [EventTriggerCollectAlterTSConfig](../E/EventTriggerCollectAlterTSConfig.md)
- Called from (representative examples):
  - [AlterTSConfiguration](../A/AlterTSConfiguration.md)

## Notes and Other Information
- This is a static function, only accessible within the tsearchcmds.c file
- Uses batch insertion with TupleTableSlots for performance optimization when inserting many tuples
- Supports three distinct operation modes controlled by stmt->override and stmt->replace flags
- Automatically handles sequence numbers (mapseqno) for dictionary ordering within token types
- Integrates with PostgreSQL's event trigger system via EventTriggerCollectAlterTSConfig
- Uses systable scan operations with proper indexing for efficient catalog table access
- Memory management includes proper cleanup of allocated TupleTableSlots
- Part of PostgreSQL's comprehensive text search configuration management system

## Simplified Source

```c
static void
MakeConfigurationMapping(AlterTSConfigurationStmt *stmt,
                        HeapTuple tup, Relation relMap)
{
    Form_pg_ts_config tsform;
    Oid cfgId;
    ScanKeyData skey[2];
    SysScanDesc scan;
    HeapTuple maptup;
    Oid prsId;
    List *tokens = NIL;
    int ntoken;
    Oid *dictIds;
    int ndict;
    ListCell *c;
    CatalogIndexState indstate;

    // Extract configuration info
    tsform = (Form_pg_ts_config) GETSTRUCT(tup);
    cfgId = tsform->oid;
    prsId = tsform->cfgparser;

    // Get valid token types and dictionary OIDs
    tokens = getTokenTypes(prsId, stmt->tokentype);
    ntoken = list_length(tokens);

    // If override mode, delete existing mappings for these tokens
    if (stmt->override)
    {
        foreach(c, tokens)
        {
            TSTokenTypeItem *ts = (TSTokenTypeItem *) lfirst(c);

            // Set up scan keys for this token type
            ScanKeyInit(&skey[0], Anum_pg_ts_config_map_mapcfg,
                        BTEqualStrategyNumber, F_OIDEQ,
                        ObjectIdGetDatum(cfgId));
            ScanKeyInit(&skey[1], Anum_pg_ts_config_map_maptokentype,
                        BTEqualStrategyNumber, F_INT4EQ,
                        Int32GetDatum(ts->num));

            // Delete existing mappings for this token
            scan = systable_beginscan(relMap, TSConfigMapIndexId, true,
                                      NULL, 2, skey);
            while (HeapTupleIsValid((maptup = systable_getnext(scan))))
                CatalogTupleDelete(relMap, &maptup->t_self);
            systable_endscan(scan);
        }
    }

    // Convert dictionary names to OIDs
    ndict = list_length(stmt->dicts);
    dictIds = (Oid *) palloc(sizeof(Oid) * ndict);
    int i = 0;
    foreach(c, stmt->dicts)
    {
        List *names = (List *) lfirst(c);
        dictIds[i] = get_ts_dict_oid(names, false);
        i++;
    }

    indstate = CatalogOpenIndexes(relMap);

    if (stmt->replace)
    {
        // Replace mode: update existing dictionary references
        Oid dictOld = dictIds[0], dictNew = dictIds[1];

        // Scan all mappings for this configuration
        ScanKeyInit(&skey[0], Anum_pg_ts_config_map_mapcfg,
                    BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(cfgId));

        scan = systable_beginscan(relMap, TSConfigMapIndexId, true,
                                  NULL, 1, skey);

        while (HeapTupleIsValid((maptup = systable_getnext(scan))))
        {
            Form_pg_ts_config_map cfgmap = (Form_pg_ts_config_map) GETSTRUCT(maptup);

            // Check if this is a target token type (if specified)
            if (tokens)
            {
                bool tokmatch = false;
                foreach(c, tokens)
                {
                    TSTokenTypeItem *ts = (TSTokenTypeItem *) lfirst(c);
                    if (cfgmap->maptokentype == ts->num)
                    {
                        tokmatch = true;
                        break;
                    }
                }
                if (!tokmatch)
                    continue;
            }

            // Replace the dictionary if it matches
            if (cfgmap->mapdict == dictOld)
            {
                Datum repl_val[Natts_pg_ts_config_map];
                bool repl_null[Natts_pg_ts_config_map];
                bool repl_repl[Natts_pg_ts_config_map];
                HeapTuple newtup;

                // Set up replacement values
                memset(repl_val, 0, sizeof(repl_val));
                memset(repl_null, false, sizeof(repl_null));
                memset(repl_repl, false, sizeof(repl_repl));

                repl_val[Anum_pg_ts_config_map_mapdict - 1] = ObjectIdGetDatum(dictNew);
                repl_repl[Anum_pg_ts_config_map_mapdict - 1] = true;

                // Update the tuple
                newtup = heap_modify_tuple(maptup, RelationGetDescr(relMap),
                                           repl_val, repl_null, repl_repl);
                CatalogTupleUpdateWithInfo(relMap, &newtup->t_self, newtup, indstate);
            }
        }
        systable_endscan(scan);
    }
    else
    {
        // Insert new mappings using batch insertion
        TupleTableSlot **slot;
        int slotCount = 0;
        int nslots;

        // Allocate slots for batch insertion
        nslots = Min(ntoken * ndict,
                     MAX_CATALOG_MULTI_INSERT_BYTES / sizeof(FormData_pg_ts_config_map));
        slot = palloc(sizeof(TupleTableSlot *) * nslots);
        for (i = 0; i < nslots; i++)
            slot[i] = MakeSingleTupleTableSlot(RelationGetDescr(relMap),
                                               &TTSOpsHeapTuple);

        // Create mapping entries for each token-dictionary combination
        foreach(c, tokens)
        {
            TSTokenTypeItem *ts = (TSTokenTypeItem *) lfirst(c);

            for (int j = 0; j < ndict; j++)
            {
                // Prepare tuple slot
                ExecClearTuple(slot[slotCount]);
                memset(slot[slotCount]->tts_isnull, false,
                       slot[slotCount]->tts_tupleDescriptor->natts * sizeof(bool));

                // Set tuple values
                slot[slotCount]->tts_values[Anum_pg_ts_config_map_mapcfg - 1] = ObjectIdGetDatum(cfgId);
                slot[slotCount]->tts_values[Anum_pg_ts_config_map_maptokentype - 1] = Int32GetDatum(ts->num);
                slot[slotCount]->tts_values[Anum_pg_ts_config_map_mapseqno - 1] = Int32GetDatum(j + 1);
                slot[slotCount]->tts_values[Anum_pg_ts_config_map_mapdict - 1] = ObjectIdGetDatum(dictIds[j]);

                ExecStoreVirtualTuple(slot[slotCount]);
                slotCount++;

                // Insert batch when slots are full
                if (slotCount == nslots)
                {
                    CatalogTuplesMultiInsertWithInfo(relMap, slot, slotCount, indstate);
                    slotCount = 0;
                }
            }
        }

        // Insert remaining tuples
        if (slotCount > 0)
            CatalogTuplesMultiInsertWithInfo(relMap, slot, slotCount, indstate);

        // Clean up slots
        for (i = 0; i < nslots; i++)
            ExecDropSingleTupleTableSlot(slot[i]);
    }

    CatalogCloseIndexes(indstate);

    // Trigger event tracking
    EventTriggerCollectAlterTSConfig(stmt, cfgId, dictIds, ndict);
}
```