# print_rmgr_list

## Location
src/bin/pg_waldump/pg_waldump.c: 98 - 112

## Overview
A utility function that prints the names of all built-in PostgreSQL resource managers (RMGRs) to standard output.

## Definition
```c
static void print_rmgr_list(void)
```

## Detailed Description
The print_rmgr_list function iterates through all built-in PostgreSQL resource managers and prints their names to standard output. Resource managers are components responsible for handling different types of WAL (Write-Ahead Log) records. This function is typically used in pg_waldump to provide users with a list of available resource manager names, which can be useful for filtering WAL records by specific resource manager types. The function uses the GetRmgrDesc() function to retrieve the resource manager descriptor for each ID and then prints the rm_name field.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - RM_MAX_BUILTIN_ID (constant defining the maximum built-in resource manager ID)
  - [GetRmgrDesc](../G/GetRmgrDesc.md) (function to get resource manager descriptor by ID)
- Called from (representative examples):
  - [main](../m/main.md) (called in pg_waldump.c:946)

## Notes and Other Information
- This function only lists built-in resource managers, not custom or extension-provided ones
- The output format is simple: one resource manager name per line
- Commonly used with command-line options like --rmgr-list in pg_waldump
- Resource manager names include system components like 'Heap', 'Btree', 'Hash', 'Gin', 'Gist', 'Sequence', 'SPGist', 'BRIN', 'CommitTs', 'ReplicationOrigin', 'Generic', 'LogicalMessage', etc.