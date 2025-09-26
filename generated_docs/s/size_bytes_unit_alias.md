# size_bytes_unit_alias

## Location
[src/backend/utils/adt/dbsize.c:60-73](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/dbsize.c#L60-L73)

## Overview
A structure that defines alternative unit names (aliases) accepted by the  function, mapping them to corresponding entries in the main size units array.

## Definition

```c
struct size_bytes_unit_alias
{
	const char *alias;
	int			unit_index;		/* corresponding size_pretty_units element */
};
```
## Detailed Description
The  structure provides a mechanism for supporting alternative unit names in PostgreSQL's size parsing functionality. It enables the  function to accept additional unit abbreviations beyond the primary names defined in . This structure acts as a lookup table that maps alias names to indices in the main units array, allowing for flexible input parsing while maintaining a single source of truth for unit definitions.

The primary use case is to support common abbreviations like "B" for "bytes", providing user-friendly input options without duplicating unit logic. When  encounters a unit string that doesn't match the primary unit names, it searches through the alias table to find alternative representations.

## Parameters / Member Variables
- `*alias`: String representation of the alternative unit name (e.g., "B" as an alias for "bytes")
- `unit_index`: Zero-based index into the  array, indicating which primary unit this alias corresponds to
## Dependencies
- Functions called/Symbols referenced:
  - References  array through 
  - Used as array element type in 
- Called from (representative examples):
  -  (iterates through alias array during unit parsing)

## Notes and Other Information
- Currently only defines "B" as an alias for "bytes" (index 0), but the structure supports additional aliases
- The alias lookup occurs only after the primary unit names have been checked and no match is found
- When adding new aliases, documentation and error messages in  should be updated to reflect available options
- The structure enables case-insensitive matching through  in the parsing logic
- The alias table is terminated with a NULL alias entry, similar to the main units array
- Provides a clean separation between primary unit definitions and alternative naming conventions