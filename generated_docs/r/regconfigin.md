# regconfigin

## Location
[src/backend/utils/adt/regproc.c:1321-1358](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L1321-L1358)

## Overview
Converts text search configuration names to their corresponding OID, serving as the input function for the regconfig data type.

## Definition


## Detailed Description
The  function is PostgreSQL's input function for the regconfig data type, which represents text search configuration identifiers. The function accepts several input formats:

1. **Dash ("-")**: Represents unknown/invalid configuration (returns OID 0)
2. **Numeric OID**: Direct OID input for symmetry with output routine
3. **Configuration name**: Text search configuration name that gets resolved to its OID

The function performs name resolution by parsing the input into qualified name components and searching the pg_ts_config system catalog within the current search path. In bootstrap mode, only numeric OIDs are accepted since catalog lookups are not available.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro
  - First argument (index 0): C string containing the text search configuration name or OID
  - : Error context for handling conversion failures

## Dependencies
- Functions called/Symbols referenced:
  - : Handles "-" and numeric OID parsing
  - : Macro to return an OID from a PostgreSQL function
  - : Checks if PostgreSQL is in bootstrap mode
  - : Parses qualified name string into list components
  - : Looks up text search configuration OID by name
  - : Error return function with context support
  - : Converts name list back to string for error messages
- Called from (representative examples):
  - No direct references found in the codebase (called by PostgreSQL's type system)

## Notes and Other Information
- Input function for the regconfig data type representing text search configurations
- Supports multiple input formats for flexibility (names, OIDs, dash for unknown)
- Performs catalog lookup with search path resolution for named configurations
- Bootstrap mode limitation: only accepts numeric OIDs during database initialization
- Part of PostgreSQL's full-text search system infrastructure
- Located in src/backend/utils/adt/regproc.c
- Uses error context for proper error handling and reporting