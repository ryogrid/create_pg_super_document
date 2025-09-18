# MakeConfigurationMapping

## Location
src/backend/commands/tsearchcmds.c: 1288 - 1490

## Overview
A static function that implements the core logic for ALTER TEXT SEARCH CONFIGURATION ADD/ALTER MAPPING commands, managing token-to-dictionary mappings in PostgreSQL's text search system.

## Definition


## Detailed Description
This function handles the complex process of adding or modifying token-to-dictionary mappings for text search configurations. It supports three main operations: 1) Adding new mappings (default mode), 2) Replacing specific dictionaries in existing mappings (replace mode), and 3) Overriding existing mappings for specified token types (override mode). The function first validates token types using getTokenTypes, then processes dictionary names to get their OIDs. For override operations, it deletes existing mappings for the specified tokens. For replace operations, it scans existing mappings and updates dictionary references. For new mappings, it uses batch insertion with TupleTableSlots for optimal performance, inserting multiple tuples per batch operation.

## Parameters / Member Variables
- : AlterTSConfigurationStmt structure containing the SQL command details including token types, dictionaries, and operation flags (override, replace)
- : HeapTuple representing the text search configuration record from pg_ts_config
- : Relation handle for the pg_ts_config_map catalog table where mappings are stored

## Dependencies
- Functions called/Symbols referenced:
  - AlterTSConfigurationStmt
  - Form_pg_ts_config
  - [getTokenTypes](../g/getTokenTypes.md)
  - TSTokenTypeItem
  - [systable_beginscan](../s/systable_beginscan.md), systable_getnext, systable_endscan
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md), CatalogTupleUpdateWithInfo
  - [get_ts_dict_oid](../g/get_ts_dict_oid.md)
  - [CatalogOpenIndexes](../C/CatalogOpenIndexes.md), CatalogCloseIndexes
  - [MakeSingleTupleTableSlot](MakeSingleTupleTableSlot.md), ExecDropSingleTupleTableSlot
  - ExecClearTuple, ExecStoreVirtualTuple
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