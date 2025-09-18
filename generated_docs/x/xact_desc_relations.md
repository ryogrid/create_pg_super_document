# xact_desc_relations

## Location
src/backend/access/rmgrdesc/xactdesc.c: 282 - 300

## Overview
xact_desc_relations is a static helper function that formats relation file locator information into a human-readable string for WAL record descriptions.

## Definition
```c
static void xact_desc_relations(StringInfo buf, char *label, int nrels, RelFileLocator *xlocators)
```

## Detailed Description
This utility function is used internally by the xact description functions to append formatted relation information to output buffers. It converts an array of RelFileLocator structures into readable file paths and appends them to a StringInfo buffer with an appropriate label. The function is essential for making WAL record descriptions human-readable in tools like pg_waldump and server logs. It only processes and outputs information when there are actually relations to describe (nrels > 0).

## Parameters / Member Variables
- `buf`: StringInfo buffer to append the formatted relation information to
- `label`: Descriptive label to prefix the relation list (e.g., "rels", "abort rels")
- `nrels`: Number of relations in the xlocators array
- `xlocators`: Array of RelFileLocator structures representing the relations

## Dependencies
- Functions called/Symbols referenced:
  - appendStringInfo
  - relpathperm
  - pfree
  - MAIN_FORKNUM
- Called from (representative examples):
  - xact_desc_commit
  - xact_desc_abort
  - xact_desc_prepare (called twice for commit and abort relations)

## Notes and Other Information
- Static function, only used within xactdesc.c
- Uses relpathperm to generate permanent file paths for relations
- Properly manages memory by freeing the path strings returned by relpathperm
- Output format: "; [label]: path1 path2 path3"
- Only outputs when nrels > 0, avoiding empty sections in descriptions
- Uses MAIN_FORKNUM as the default fork for path generation
- Critical for debugging and WAL analysis by providing readable relation references