# makeParserDependencies

## Location
[src/backend/commands/tsearchcmds.c:137-183](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tsearchcmds.c#L137-L183)

## Overview
This function creates pg_depend entries for a new text search parser, establishing all necessary dependencies including namespace, functions, and extension relationships.

## Definition
```c
static ObjectAddress makeParserDependencies(HeapTuple tuple)
```

## Detailed Description
The function establishes a complete dependency graph for a newly created text search parser by recording dependencies in the pg_depend system catalog. It extracts the parser information from the provided HeapTuple and creates dependency records for the parser's namespace, all required functions (prsstart, prstoken, prsend, prslextype), and optionally the prsheadline function if present. This ensures proper cascading behavior when any of these dependent objects are dropped.

The function also records the parser's membership in the current extension (if executed within an extension context) and uses normal dependency strength for all function and namespace dependencies, meaning the parser will be automatically dropped if any of its dependent functions or namespace are dropped.

## Parameters / Member Variables
- `tuple`: HeapTuple containing the pg_ts_parser row data for the new parser

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_ts_parser: Type cast to access parser tuple fields
  - ObjectAddressSet: Sets up object address structures
  - [recordDependencyOnCurrentExtension](../r/recordDependencyOnCurrentExtension.md): Records extension membership
  - [new_object_addresses](../n/new_object_addresses.md): Creates new ObjectAddresses collection
  - [add_exact_object_address](../a/add_exact_object_address.md): Adds object to dependency collection
  - [record_object_address_dependencies](../r/record_object_address_dependencies.md): Records all dependencies with specified strength
  - [free_object_addresses](../f/free_object_addresses.md): Cleans up ObjectAddresses collection
  - OidIsValid: Checks if optional prsheadline function is provided
- Called from (representative examples):
  - [DefineTSParser](../D/DefineTSParser.md): Called after inserting new parser tuple to establish dependencies

## Notes and Other Information
- This is a static function, only accessible within tsearchcmds.c
- The prsheadline function is optional and only recorded as a dependency if present (OidIsValid check)
- All dependencies use DEPENDENCY_NORMAL strength, ensuring proper cascading deletion behavior
- The function returns the ObjectAddress of the parser itself for potential use by callers
- Dependencies are recorded for: namespace, prsstart, prstoken, prsend, prslextype, and optionally prsheadline functions
- Extension dependency is recorded separately from the function/namespace dependencies