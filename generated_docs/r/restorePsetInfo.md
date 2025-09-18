# restorePsetInfo

## Location
[src/bin/psql/command.c:5122-5147](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L5122-L5147)

## Overview
Restores a printQueryOpt structure from a previously saved copy, freeing the old data and transferring ownership of dynamically allocated strings from the saved copy.

## Definition


## Detailed Description
The restorePsetInfo function performs the reverse operation of savePsetInfo by restoring a printQueryOpt structure from a previously saved copy. It first frees all dynamically allocated string data in the current structure to prevent memory leaks, then performs a flat copy of the entire saved structure using memcpy. Finally, it frees the save structure itself while leaving the transferred string pointers intact in the restored structure.

This function is designed to work in tandem with savePsetInfo, providing a complete save/restore mechanism for PostgreSQL query printing options in psql. The function includes the same assertions as savePsetInfo to ensure that footers and translate_columns are never set in psql's context.

## Parameters / Member Variables
- : Pointer to the printQueryOpt structure to be restored (destination)
- : Pointer to the previously saved printQueryOpt structure (source, will be freed)

## Dependencies
- Functions called/Symbols referenced:
  - free (for memory deallocation)
  - memcpy (for structure copying)
  - [printQueryOpt](../p/printQueryOpt.md) (structure type)
- Called from (representative examples):
  - [process_command_g_options](../p/process_command_g_options.md)
  - [SendQuery](../S/SendQuery.md)

## Notes and Other Information
- Must be called with a save pointer that was created by savePsetInfo
- Frees the save structure but transfers ownership of its string data to popt
- The function handles NULL string pointers safely through free()
- Contains assertions that footers and translate_columns are always NULL in psql context
- The topt.line_style field points to constant data that doesn't require special handling
- After this function completes, the save pointer becomes invalid and should not be used
- Essential for implementing temporary modifications to print settings that can be reverted