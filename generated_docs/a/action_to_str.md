# action_to_str

## Location
[src/bin/pg_rewind/filemap.c:473-498](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/filemap.c#L473-L498)

## Overview
Converts a file_action_t enumeration value to its corresponding string representation for debugging and logging purposes in pg_rewind.

## Definition
static const char *action_to_str(file_action_t action)

## Detailed Description
This function is a utility function used in pg_rewind to convert file action enumeration values into human-readable strings. It takes a file_action_t enum value and returns the corresponding string representation. This is particularly useful for debugging output and logging when pg_rewind displays what actions it will perform on files during the rewind operation.

The function uses a simple switch statement to map each file action constant to its string representation, with a default case that returns "unknown" for any unrecognized action values.

## Parameters / Member Variables
- action: The file_action_t enumeration value to convert to string

## Dependencies
- Functions called/Symbols referenced:
  - file_action_t (enum type)
  - FILE_ACTION_NONE
  - FILE_ACTION_COPY
  - FILE_ACTION_TRUNCATE
  - FILE_ACTION_COPY_TAIL
  - FILE_ACTION_CREATE
  - FILE_ACTION_REMOVE
- Called from (representative examples):
  - [print_filemap](../p/print_filemap.md)

## Notes and Other Information
- This is a static function, so it's only visible within the filemap.c compilation unit
- Returns "unknown" for any file action values not explicitly handled in the switch statement
- Used primarily for debugging and user-friendly output when showing the file operations that pg_rewind will perform
- Part of the pg_rewind utility which helps recover from failover by rewinding a PostgreSQL server to an earlier state

## Simplified Source

```c
static const char *
action_to_str(file_action_t action)
{
    // Convert file action enum to string representation
    switch (action)
    {
        case FILE_ACTION_NONE:
            return "NONE";
        case FILE_ACTION_COPY:
            return "COPY";
        case FILE_ACTION_TRUNCATE:
            return "TRUNCATE";
        case FILE_ACTION_COPY_TAIL:
            return "COPY_TAIL";
        case FILE_ACTION_CREATE:
            return "CREATE";
        case FILE_ACTION_REMOVE:
            return "REMOVE";
        default:
            return "unknown";
    }
}
```