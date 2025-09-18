# regdictionaryout

## Location
src/backend/utils/adt/regproc.c: 1469 - 1515

## Overview
Converts a text search dictionary OID to its corresponding dictionary name string for output display.

## Definition


## Detailed Description
The  function is part of PostgreSQL's regtype system for text search dictionaries. It takes an OID (Object Identifier) representing a text search dictionary and converts it to a human-readable string representation. The function handles multiple output scenarios based on the dictionary's visibility and existence:

1. **Invalid OID**: Returns "-" for  
2. **Valid, visible dictionary**: Returns just the dictionary name if it's in the current search path
3. **Valid, non-visible dictionary**: Returns schema-qualified name (e.g., "schema.dictname")
4. **Non-existent OID**: Returns the numeric OID as a string

The function uses the system catalog  to look up dictionary details and applies PostgreSQL's visibility rules to determine whether schema qualification is necessary for unambiguous identification.

## Parameters / Member Variables
- **Input**: OID of the text search dictionary (accessed via )
- **Return**:  containing a C string representation of the dictionary

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract OID argument from function call
  -  - Search system catalog cache for dictionary entry
  -  - Extract tuple structure from heap tuple
  -  - Check if dictionary is visible in current search path
  -  - Get schema name from namespace OID
  -  - Properly quote and qualify identifiers
  -  - Release system cache tuple
  -  - Return string result
- Called from:
  - System catalog output functions (indirectly via SQL system)

## Notes and Other Information
- This function is the output counterpart to  which parses dictionary names
- Uses PostgreSQL's visibility rules to determine when schema qualification is needed
- Falls back to numeric representation for non-existent OIDs rather than throwing an error
- Memory management uses  for result allocation
- Part of the regtype system that provides user-friendly representations of internal OIDs
- Critical for displaying dictionary references in SQL output and system catalogs