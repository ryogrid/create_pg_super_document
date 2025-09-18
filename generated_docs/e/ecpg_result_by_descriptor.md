# ecpg_result_by_descriptor

## Location
src/interfaces/ecpg/ecpglib/descriptor.c: 54 - 63

## Overview
A legacy internal convenience function that retrieves the PGresult associated with a named descriptor in the ECPG library.

## Definition
```c
static PGresult *ecpg_result_by_descriptor(int line, const char *name)
```

## Detailed Description
The `ecpg_result_by_descriptor` function provides a convenient way to access the PostgreSQL result set (PGresult) associated with a specific descriptor. It works by first locating the descriptor using `ecpg_find_desc` and then returning the result field from that descriptor. This function serves as a bridge between descriptor names and their associated query results, simplifying result access in ECPG applications. The function includes a comment indicating it is an "old internal convenience function that might go away later," suggesting it may be deprecated in future versions.

## Parameters / Member Variables
- `line`: The line number in the source code where this function is called, used for error reporting and debugging purposes
- `name`: The name of the descriptor whose associated PGresult should be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - ecpg_find_desc
  - descriptor (structure access)
- Called from (representative examples):
  - ECPGget_desc_header
  - ECPGget_desc

## Notes and Other Information
- This function is declared as static, meaning it has internal linkage and is only accessible within the descriptor.c compilation unit
- Returns NULL if the descriptor is not found or if the descriptor lookup fails
- The function is marked as a legacy convenience function that may be removed in future versions
- Provides a simplified interface for accessing result sets without requiring direct descriptor manipulation
- The line parameter follows the ECPG convention of tracking source code locations for debugging and error reporting