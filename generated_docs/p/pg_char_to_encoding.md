# pg_char_to_encoding

## Location
src/common/encnames.c: 549 - 586

## Overview
Searches for an encoding by its name and returns the corresponding encoding ID, or -1 if the encoding is not recognized.

## Definition

```c
int
pg_char_to_encoding(const char *name)
```
## Detailed Description
The `pg_char_to_encoding` function performs a binary search through PostgreSQL's internal encoding name table (`pg_encname_tbl`) to find a matching encoding name. The function first normalizes the input name using `clean_encoding_name` to ensure consistent matching regardless of case or special characters in the encoding name.

The search algorithm uses binary search for efficient lookup in the sorted encoding table. Each entry in the table consists of a normalized encoding name and its corresponding PostgreSQL encoding constant. The function returns the encoding ID if found, or -1 if the encoding name is not recognized or if the input is invalid.

## Parameters / Member Variables
- `name`: The encoding name string to search for (e.g., "UTF-8", "iso-8859-1", "EUC-JP")

## Dependencies
- Functions called/Symbols referenced:
  - lengthof (macro to get array length)
  - clean_encoding_name (normalizes encoding name)
  - strcmp (standard C library string comparison)
- Data structures:
  - pg_encname_tbl (static table of encoding names and IDs)
  - pg_encname (structure containing name and encoding pairs)
- Constants:
  - NAMEDATALEN (maximum length for names)
- Called from (representative examples):
  - CreateConversionCommand
  - ProcessCopyOptions
  - pg_convert
  - PQenv2encoding

## Notes and Other Information
- Returns -1 for NULL input, empty strings, or names longer than NAMEDATALEN
- The encoding table (`pg_encname_tbl`) contains normalized lowercase names without special characters
- Uses binary search for O(log n) lookup performance
- All encoding names in the table are pre-normalized and sorted alphabetically
- The function is widely used throughout PostgreSQL for encoding name resolution
- Input names are automatically normalized before comparison, allowing flexible input formats