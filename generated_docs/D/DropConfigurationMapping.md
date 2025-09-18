# DropConfigurationMapping

## Location
src/backend/commands/tsearchcmds.c: 1491 - 1564

## Overview
A static function that implements the ALTER TEXT SEARCH CONFIGURATION DROP MAPPING command, removing specified token-to-dictionary mappings from a text search configuration.

## Definition


## Detailed Description
This function removes token-to-dictionary mappings from the pg_ts_config_map catalog table for a specified text search configuration. It first validates the token types using getTokenTypes to ensure they exist in the parser's lexical type list. For each valid token type, it performs a systematic scan of the mapping table using the configuration ID and token type as search keys. All matching mapping entries are deleted using CatalogTupleDelete. The function provides flexible error handling based on the missing_ok flag: when set to false, it raises an error if a mapping doesn't exist; when true, it issues a notice and continues processing remaining tokens.

## Parameters / Member Variables
- : AlterTSConfigurationStmt structure containing the SQL command details, including token types and the missing_ok flag for error handling
- : HeapTuple representing the text search configuration record from pg_ts_config
- : Relation handle for the pg_ts_config_map catalog table where mappings are stored and deleted from

## Dependencies
- Functions called/Symbols referenced:
  - AlterTSConfigurationStmt
  - Form_pg_ts_config
  - [getTokenTypes](../g/getTokenTypes.md)
  - TSTokenTypeItem
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md), systable_getnext, systable_endscan
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md)
  - [EventTriggerCollectAlterTSConfig](../E/EventTriggerCollectAlterTSConfig.md)
  - ereport (for error and notice reporting)
- Called from (representative examples):
  - [AlterTSConfiguration](../A/AlterTSConfiguration.md)

## Notes and Other Information
- This is a static function, only accessible within the tsearchcmds.c file
- Supports graceful handling of non-existent mappings through the missing_ok flag
- Uses indexed scans on TSConfigMapIndexId for efficient mapping lookup and deletion
- Integrates with PostgreSQL's event trigger system for configuration change tracking
- Provides both ERROR and NOTICE level reporting depending on missing_ok setting
- Part of PostgreSQL's text search configuration management system
- Ensures complete cleanup by deleting all mappings associated with specified token types
- Uses proper transaction-safe catalog operations for data consistency