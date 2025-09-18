# DefineTSConfiguration

## Location
src/backend/commands/tsearchcmds.c: 899 - 1107

## Overview
Creates a new text search configuration in the system catalog, optionally copying token-dictionary mappings from an existing configuration.

## Definition
```c
ObjectAddress DefineTSConfiguration(List *names, List *parameters, ObjectAddress *copied)
```

## Detailed Description
DefineTSConfiguration implements the CREATE TEXT SEARCH CONFIGURATION SQL command. It validates parameters, checks permissions, creates the configuration tuple in pg_ts_config, and optionally copies token-dictionary mappings from a source configuration. The function supports two modes: creating a configuration with a specified parser, or copying an existing configuration. It uses batch insertion for efficiency when copying large configuration maps and establishes all necessary dependency relationships.

## Parameters / Member Variables
- `names`: List containing the qualified or unqualified name components for the new configuration
- `parameters`: List of DefElem nodes containing configuration options ("parser" or "copy")
- `copied`: Output parameter set to the ObjectAddress of the copied configuration, or NULL if not copying

## Dependencies
- Functions called/Symbols referenced:
  - [QualifiedNameGetCreationNamespace](../Q/QualifiedNameGetCreationNamespace.md) (resolves namespace and name)
  - [object_aclcheck](../o/object_aclcheck.md) (checks CREATE permission on namespace)
  - [get_ts_parser_oid](../g/get_ts_parser_oid.md) (resolves parser name to OID)
  - [get_ts_config_oid](../g/get_ts_config_oid.md) (resolves source config name to OID)
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md) (allocates new OID for configuration)
  - [heap_form_tuple](../h/heap_form_tuple.md)/CatalogTupleInsert (creates configuration tuple)
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md)/ExecDropSingleTupleTableSlot (manages tuple slots)
  - [CatalogTuplesMultiInsertWithInfo](../C/CatalogTuplesMultiInsertWithInfo.md) (batch inserts map entries)
  - [makeConfigurationDependencies](../m/makeConfigurationDependencies.md) (establishes dependencies)
  - InvokeObjectPostCreateHook (triggers post-creation hooks)
  - [heap_freetuple](../h/heap_freetuple.md) (cleanup)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (SQL command processing)

## Notes and Other Information
- Validates mutually exclusive PARSER and COPY options
- Requires CREATE privilege on target namespace
- Uses batch insertion with configurable slot count for map copying efficiency
- Copies all token-dictionary mappings when using COPY option
- Returns ObjectAddress for use in dependency tracking
- Supports extension membership through makeConfigurationDependencies
- Uses RowExclusiveLock on both pg_ts_config and pg_ts_config_map relations
- Implements proper error handling for missing parsers and configurations