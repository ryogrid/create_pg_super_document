# regconfigout

## Location
[src/backend/utils/adt/regproc.c:1359-1404](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L1359-L1404)

## Overview
Converts a text search configuration OID to its corresponding configuration name string for output display.

## Definition


## Detailed Description
The  function is part of PostgreSQL's regtype system for text search configurations. It takes an OID (Object Identifier) representing a text search configuration and converts it to a human-readable string representation. The function handles three main cases:

1. **Invalid OID**: Returns "-" for 
2. **Valid, visible configuration**: Returns just the configuration name if it's in the search path
3. **Valid, non-visible configuration**: Returns schema-qualified name (e.g., "schema.configname")
4. **Non-existent OID**: Returns the numeric OID as a string

The function uses the system catalog  to look up configuration details and applies PostgreSQL's visibility rules to determine whether schema qualification is necessary.

## Parameters / Member Variables
- **Input**: OID of the text search configuration (accessed via )
- **Return**:  containing a C string representation of the configuration

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract OID argument from function call
  -  - Search system catalog cache
  -  - Extract tuple structure
  -  - Check if configuration is visible in search path
  -  - Get schema name from namespace OID
  -  - Properly quote and qualify identifiers
  -  - Release system cache tuple
  -  - Return string result
- Called from:
  - System catalog output functions (indirectly via SQL system)

## Notes and Other Information
- This function is the output counterpart to  which parses configuration names
- Uses PostgreSQL's visibility rules to determine when schema qualification is needed
- Falls back to numeric representation for non-existent OIDs rather than throwing an error
- Memory management uses  for result allocation
- Part of the regtype system that provides user-friendly representations of internal OIDs