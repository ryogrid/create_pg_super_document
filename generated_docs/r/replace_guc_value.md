# replace_guc_value

## Location
[src/bin/initdb/initdb.c:525-640](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L525-L640)

## Overview
Modifies an array of configuration lines by replacing or adding a GUC (Grand Unified Configuration) parameter assignment with proper value quoting and comment preservation.

## Definition

```c
static char **
replace_guc_value(char **lines, const char *guc_name, const char *guc_value,
				  bool mark_as_comment)
```
## Detailed Description
This function processes PostgreSQL configuration files by finding existing GUC parameter assignments and replacing them with new values, or appending new assignments if none exist. It handles complex formatting requirements including proper quoting of values that require it, preservation of original comments with indentation, and optional commenting out of assignments. The function is designed to maintain the readability and structure of postgresql.conf files during database initialization. It assumes at most one matching assignment exists and processes lines in order until a match is found.

## Parameters / Member Variables
- `**lines`: Array of malloc'd strings representing configuration file lines, terminated by NULL pointer
- `*guc_name`: The name of the GUC parameter to find and replace
- `*guc_value`: The new value to assign to the parameter
- `mark_as_comment`: Boolean flag to prefix the replacement line with '#' to comment it out
## Dependencies
- Functions called/Symbols referenced:
  -  (standard library function)
  -  (PostgreSQL buffer management)
  -  (PostgreSQL buffer operations) 
  -  (PostgreSQL buffer operations)
  -  (PostgreSQL buffer operations)
  -  (PostgreSQL configuration utility)
  -  (PostgreSQL string escaping)
  -  (standard library function)
  -  (PostgreSQL case-insensitive string comparison)
  -  (standard library function)
  -  (PostgreSQL memory management)
  -                total        used        free      shared  buff/cache   available
Mem:        32819380     4965904    25385848        3040     2467628    27471256
Swap:        8388608           0     8388608 (standard library function)
- Called from (representative examples):
  -  (extensively used for various configuration parameters)
  - Used with  macro

## Notes and Other Information
- Preserves original comment indentation using tab/space calculations
- Handles both commented and uncommented existing assignments  
- Automatically quotes values when necessary using PostgreSQL quoting rules
- Appends new assignments if no existing match is found
- Uses case-insensitive matching for parameter names but preserves original casing
- Part of initdb's configuration file processing system
- Critical for setting up proper PostgreSQL server configuration during initialization