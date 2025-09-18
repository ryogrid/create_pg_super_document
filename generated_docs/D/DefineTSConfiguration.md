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
  - QualifiedNameGetCreationNamespace (resolves namespace and name)
  - object_aclcheck (checks CREATE permission on namespace)
  - get_ts_parser_oid (resolves parser name to OID)
  - get_ts_config_oid (resolves source config name to OID)
  - GetNewOidWithIndex (allocates new OID for configuration)
  - heap_form_tuple/CatalogTupleInsert (creates configuration tuple)
  - MakeSingleTupleTableSlot/ExecDropSingleTupleTableSlot (manages tuple slots)
  - CatalogTuplesMultiInsertWithInfo (batch inserts map entries)
  - makeConfigurationDependencies (establishes dependencies)
  - InvokeObjectPostCreateHook (triggers post-creation hooks)
  - heap_freetuple (cleanup)
- Called from (representative examples):
  - ProcessUtilitySlow (SQL command processing)

## Notes and Other Information
- Validates mutually exclusive PARSER and COPY options
- Requires CREATE privilege on target namespace
- Uses batch insertion with configurable slot count for map copying efficiency
- Copies all token-dictionary mappings when using COPY option
- Returns ObjectAddress for use in dependency tracking
- Supports extension membership through makeConfigurationDependencies
- Uses RowExclusiveLock on both pg_ts_config and pg_ts_config_map relations
- Implements proper error handling for missing parsers and configurations