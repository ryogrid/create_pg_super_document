# simple_string_list_not_touched

## Location
src/fe_utils/simple_list.c: 144 - 161

## Overview
Finds and returns the first untouched entry in a SimpleStringList, used for validation and error checking.

## Definition
const char *simple_string_list_not_touched(SimpleStringList *list)

## Detailed Description
This function traverses a SimpleStringList to find the first list cell that has not been marked as 'touched'. It iterates through the linked list starting from the head, checking each cell's touched flag. When it finds a cell where the touched flag is false, it returns the value (string) stored in that cell. This functionality is typically used for validation purposes to identify unused or unprocessed entries in a list.

## Parameters / Member Variables
- : Pointer to the SimpleStringList structure to search through

## Dependencies
- Functions called/Symbols referenced:
  - None (simple traversal logic)
- Data structures used:
  - SimpleStringList
  - SimpleStringListCell
- Called from (representative examples):
  - StrictNamesCheck (src/bin/pg_dump/pg_backup_archiver.c:2884, 2891, 2898, 2905, 2912)

## Notes and Other Information
- Returns NULL if all entries in the list have been touched or if the list is empty
- The function returns a const char pointer to the string value, not a copy
- Primarily used in pg_dump utilities for strict name checking and validation
- The 'touched' flag is used to track which entries have been processed or validated
- Located in src/fe_utils/simple_list.c:144-161