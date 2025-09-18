# parse_policy_command

## Location
src/backend/commands/policy.c: 108 - 136

## Overview
A helper function that converts full policy command strings ('all', 'select', 'insert', 'update', 'delete') to their corresponding single-character representations used internally by PostgreSQL's row-level security system.

## Definition
```c
static char parse_policy_command(const char *cmd_name)
```

## Detailed Description
This function serves as a string-to-character converter for row-level security policy commands. It takes human-readable command names from SQL policy statements and transforms them into the compact single-character format used internally by PostgreSQL's access control system. The function performs case-sensitive string comparison and maps each valid command to its corresponding ACL character constant.

The mapping ensures consistency with PostgreSQL's existing access control infrastructure, where permissions are represented as single characters. This design allows for efficient storage and comparison of policy commands in system catalogs.

## Parameters / Member Variables
- `cmd_name`: Null-terminated string containing the policy command name ("all", "select", "insert", "update", or "delete")

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (string comparison)
  - elog (error logging and reporting)
  - ACL_SELECT_CHR (select command character constant)
  - ACL_INSERT_CHR (insert command character constant)  
  - ACL_UPDATE_CHR (update command character constant)
  - ACL_DELETE_CHR (delete command character constant)

- Called from:
  - [CreatePolicy](../C/CreatePolicy.md) (during policy creation to parse command specifications)

## Notes and Other Information
- This is a static function, only accessible within the policy.c module
- Returns '*' for "all" command, which represents a policy that applies to all operations
- Uses PostgreSQL's existing ACL character constants for consistency with the broader permission system
- Performs strict string matching - commands must be exactly "all", "select", "insert", "update", or "delete"
- Raises ERROR-level exceptions for null input or unrecognized command names
- The character-based representation enables efficient storage in system catalogs and fast comparison operations