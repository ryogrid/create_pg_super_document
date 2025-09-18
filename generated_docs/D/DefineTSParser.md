# DefineTSParser

## Location
src/backend/commands/tsearchcmds.c: 184 - 306

## Overview
This function implements the CREATE TEXT SEARCH PARSER SQL command, creating a new text search parser object in the system catalog with all required function references and dependencies.

## Definition
```c
ObjectAddress DefineTSParser(List *names, List *parameters)
```

## Detailed Description
The function processes a CREATE TEXT SEARCH PARSER command by validating parameters, creating a new parser entry in the pg_ts_parser system catalog, and establishing all necessary dependencies. It requires superuser privileges and validates that all required parser functions (start, gettoken, end, lextypes) are provided, while the headline function is optional.

The function extracts parser function specifications from the parameters list, validates each function using get_ts_parser_func(), creates the catalog tuple, inserts it into pg_ts_parser, and establishes dependencies through makeParserDependencies(). The process ensures that the parser is properly integrated into the dependency system and extension framework.

## Parameters / Member Variables
- `names`: List of names representing the qualified parser name (schema.parser_name)
- `parameters`: List of DefElem structures specifying parser functions (start, gettoken, end, headline, lextypes)

## Dependencies
- Functions called/Symbols referenced:
  - superuser: Checks if current user has superuser privileges
  - QualifiedNameGetCreationNamespace: Resolves namespace and extracts parser name
  - GetNewOidWithIndex: Generates new OID for the parser
  - get_ts_parser_func: Validates and retrieves function OIDs for parser methods
  - heap_form_tuple: Creates heap tuple from values array
  - CatalogTupleInsert: Inserts tuple into pg_ts_parser catalog
  - makeParserDependencies: Establishes all dependency relationships
  - InvokeObjectPostCreateHook: Triggers post-creation hooks
  - heap_freetuple: Frees tuple memory
- Called from (representative examples):
  - ProcessUtilitySlow: Called during SQL command processing for CREATE TEXT SEARCH PARSER

## Notes and Other Information
- Requires superuser privileges to create text search parsers
- Four functions are mandatory: start, gettoken, end, lextypes
- The headline function is optional and can be omitted
- Parser functions are validated for correct signatures during creation
- Returns ObjectAddress of the newly created parser for dependency tracking
- Supports qualified naming (schema.parser_name) with proper namespace resolution
- Integrates with extension system and object dependency framework
- Uses row-exclusive locking on pg_ts_parser during creation