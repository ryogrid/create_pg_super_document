# compareWalFileNames

## Location
[src/backend/backup/basebackup.c:684-695](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup.c#L684-L695)

## Overview
 is a comparison function used for sorting WAL file names by their log/segment portion while ignoring the timeline portion.

## Definition


## Detailed Description
This function serves as a comparison callback for  to order WAL segment filenames. It specifically compares only the log/segment portion of WAL filenames (characters 8 and onwards) while ignoring the timeline portion at the beginning. This ensures WAL files are sorted in chronological order regardless of their timeline, which is important during base backup when WAL files from different timelines might be present but need to be sent in the correct sequence.

WAL filenames in PostgreSQL follow the format:  where:
- TTTTTTTT: Timeline (first 8 characters, ignored by this function)
- LLLLLLLL: Log file number 
- SSSSSSSS: Segment number

By comparing from character 8 onwards, this function ensures proper ordering by log/segment numbers.

## Parameters / Member Variables
- : First ListCell containing a WAL filename string to compare
- : Second ListCell containing a WAL filename string to compare

## Dependencies
- Functions called/Symbols referenced:
  - lfirst (PostgreSQL list macro)
  - strcmp (standard C library)
- Called from (representative examples):
  - [perform_base_backup](../p/perform_base_backup.md) (via list_sort)

## Notes and Other Information
- Used specifically by  in  to sort WAL files before transmission
- The +8 offset skips the timeline portion of WAL filenames to focus on chronological ordering
- Returns standard strcmp semantics: negative if a < b, zero if equal, positive if a > b
- Critical for ensuring WAL files are sent in the correct order during backup to reduce recycling risks