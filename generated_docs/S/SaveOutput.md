# SaveOutput

## Location
src/bin/pg_dump/pg_backup_archiver.c: 1715 - 1720

## Overview
A simple private function that saves and returns the current output file handle from an archive for later restoration.

## Definition
```c
static CompressFileHandle *SaveOutput(ArchiveHandle *AH)
```

## Detailed Description
The `SaveOutput` function provides a straightforward mechanism to capture and return the current output file handle associated with an archive. This function is typically used in conjunction with `RestoreOutput` to implement a save/restore pattern for temporarily redirecting archive output to different destinations.

The function simply extracts and returns the compression file handle stored in the archive's OF (Output File) field, casting it to the appropriate type. This allows calling code to maintain a reference to the current output state before potentially changing it, enabling restoration of the original output destination later.

This is part of the private archiver API and is designed to support scenarios where archive output needs to be temporarily redirected, such as when generating table of contents summaries or handling different output streams during restore operations.

## Parameters / Member Variables
- `AH`: Pointer to the ArchiveHandle structure containing the current archive state and output file handle

## Dependencies
- Functions called/Symbols referenced:
  - CompressFileHandle (type cast)
- Called from (representative examples):
  - RestoreArchive
  - PrintTOCSummary

## Notes and Other Information
- This is a private static function internal to the archiver routines
- Returns the current CompressFileHandle pointer from the archive's OF field
- Designed to work in tandem with `RestoreOutput` for save/restore output patterns
- Very lightweight function with minimal overhead
- Does not perform any validation or error checking on the returned handle
- Located in `src/bin/pg_dump/pg_backup_archiver.c:1715-1720`
- The returned handle should be stored temporarily and used with `RestoreOutput` to restore the original output state