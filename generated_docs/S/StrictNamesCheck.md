# StrictNamesCheck

## Location
src/bin/pg_dump/pg_backup_archiver.c: 2876 - 2925

## Overview
This function validates that all explicitly named database objects (schemas, tables, indexes, functions, triggers) specified in restore options were actually found and processed during restoration when strict name checking is enabled.

## Definition


## Detailed Description
The  function enforces strict validation of object names when the  option is enabled in restore operations. It iterates through various object name lists in the restore options and verifies that every explicitly specified object was actually encountered and processed during the restoration.

The function uses the  utility to identify any names in the lists that were never marked as "touched" during processing. If any untouched names are found, the function calls  to terminate the restore operation with an appropriate error message.

This strict checking is particularly useful in automated environments where it's important to ensure that all intended objects were successfully restored, preventing silent failures where some objects might be missing from the archive or incorrectly named.

## Parameters / Member Variables
- : RestoreOptions structure containing configuration and object name lists for the restoration process

## Dependencies
- Functions called/Symbols referenced:
  - Assert (assertion macro)
  - simple_string_list_not_touched (utility function to find unprocessed list items)
  - pg_fatal (PostgreSQL error handling function)
  - RestoreOptions (struct type)
- Called from (representative examples):
  - ProcessArchiveRestoreOptions (during restore option processing)
  - PrintTOCSummary (when printing table of contents summary)

## Notes and Other Information
- Only executes when  is true
- Checks five categories of database objects: schemas, tables, indexes, functions, and triggers
- Uses the "touched" mechanism to track which named objects were actually processed
- Terminates the entire restore operation on the first missing object found
- Provides specific error messages indicating both the object type and name that wasn't found
- This validation occurs after the main restoration processing is complete
- Helps prevent silent failures in scripted backup/restore scenarios