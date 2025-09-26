# parse_filename_for_nontemp_relation

## Location
[src/backend/storage/file/reinit.c:380-453](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/reinit.c#L380-L453)

## Overview
parse_filename_for_nontemp_relation is a utility function that parses PostgreSQL relation filenames to extract the relation number, fork type, and segment number, ensuring the filename follows the correct format for non-temporary relations.

## Definition
```c
bool parse_filename_for_nontemp_relation(const char *name, RelFileNumber *relnumber, 
                                        ForkNumber *fork, unsigned *segno)
```

## Detailed Description
This function performs comprehensive parsing and validation of PostgreSQL relation filenames, which follow specific naming conventions:

- **Basic format**: `<RelFileNumber>[_<ForkName>][.<SegmentNumber>]`
- **Examples**: `16384`, `16384_fsm`, `16384.1`, `16384_vm.2`

### Parsing Algorithm:
1. **Leading digit validation**: Rejects filenames starting with '0' or non-digits to prevent ambiguous interpretations
2. **RelFileNumber extraction**: Parses the leading numeric portion, validating it's within PostgreSQL's valid range (1 to PG_UINT32_MAX)
3. **Fork name detection**: Identifies optional fork suffixes (e.g., _fsm, _vm, _init) using `forkname_chars`
4. **Segment number parsing**: Extracts optional segment numbers for large relations, with similar leading-zero rejection
5. **End-of-string validation**: Ensures no trailing characters remain

The function is designed to distinguish actual relation files from stray files that might accidentally exist in database directories.

## Parameters / Member Variables
- `name`: The filename to parse (e.g., "16384_init.1")
- `relnumber`: Output parameter for the extracted RelFileNumber
- `fork`: Output parameter for the fork type (MAIN_FORKNUM, FSM_FORKNUM, etc.)
- `segno`: Output parameter for the segment number (0 if no segment specified)

**Return Value**: `true` if the filename is valid, `false` otherwise

## Dependencies
- Functions called/Symbols referenced:
  - `strtoul`: String-to-unsigned-long conversion with error checking
  - `[forkname_chars](../f/forkname_chars.md)`: Fork name identification and length calculation
  - **Constants**: `InvalidRelFileNumber`, `InvalidForkNumber`, `MAIN_FORKNUM`, `PG_UINT32_MAX`

- Called from:
  - `[ResetUnloggedRelationsInDbspaceDir](../R/ResetUnloggedRelationsInDbspaceDir.md)`: During unlogged relation processing (lines 202, 241, 293, 336)
  - `[sendDir](../s/sendDir.md)`: During base backup operations (basebackup.c:1311)

## Notes and Other Information
- Implements strict validation to prevent false positives when scanning database directories
- Rejects leading zeros in both RelFileNumber and segment numbers to ensure unique string representations
- The fork name parsing delegates to `forkname_chars` which handles the mapping between fork name strings and `ForkNumber` enums
- Segment numbers are used for relations larger than 1GB (each segment represents 1GB of data)
- Returns `InvalidRelFileNumber` and `InvalidForkNumber` as sentinel values when parsing fails
- This function is crucial for distinguishing relation files from other files that might exist in database directories
- Located in src/backend/storage/file/reinit.c:380-453