# pg_stat_file

## Location
[src/backend/utils/adt/genfile.c:413-488](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/genfile.c#L413-L488)

## Overview
A PostgreSQL system function that returns detailed file statistics including size, timestamps, and directory status, with optional missing file tolerance.

## Definition

```c
struct stat fst;
```
## Detailed Description
This function provides comprehensive file system information for a specified file path. It serves as a PostgreSQL interface to the system's  function, returning structured data about file attributes including size, access time, modification time, change/creation time, and directory status. The function supports an optional  parameter that allows graceful handling of non-existent files. The return value is a composite type (record) containing six fields with file metadata. The function handles platform differences between Unix-like systems and Windows regarding file timestamps, where Unix provides status change time while Windows provides creation time.

## Parameters / Member Variables
- : Text parameter containing the path to the file to be examined
- : Optional boolean parameter (when PG_NARGS() == 2) indicating whether to return NULL instead of raising an error if the file doesn't exist

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP (macro for extracting text argument)
  - PG_NARGS (macro to get number of arguments)
  - PG_GETARG_BOOL (macro for extracting boolean argument)
  - [convert_and_check_filename](../c/convert_and_check_filename.md) (filename validation and conversion)
  - [stat](../s/stat.md) (system call to get file statistics)
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md) (create tuple descriptor)
  - [TupleDescInitEntry](../T/TupleDescInitEntry.md) (initialize tuple descriptor entry)
  - [BlessTupleDesc](../B/BlessTupleDesc.md) (finalize tuple descriptor)
  - [Int64GetDatum](../I/Int64GetDatum.md), TimestampTzGetDatum, BoolGetDatum (data conversion functions)
  - [time_t_to_timestamptz](../t/time_t_to_timestamptz.md) (timestamp conversion)
  - S_ISDIR (macro to check if file is directory)
  - [heap_form_tuple](../h/heap_form_tuple.md) (create heap tuple)
  - [HeapTupleGetDatum](../H/HeapTupleGetDatum.md) (convert tuple to datum)
  - PG_RETURN_DATUM, PG_RETURN_NULL (return macros)
- Called from (representative examples):
  - [pg_stat_file_1arg](pg_stat_file_1arg.md) (wrapper function)

## Notes and Other Information
- Located in src/backend/utils/adt/genfile.c:413-488
- Returns a record with 6 fields: size (int8), access (timestamptz), modification (timestamptz), change (timestamptz), creation (timestamptz), isdir (bool)
- Platform-specific behavior: Unix systems populate change time (st_ctime) and set creation time to NULL; Windows systems populate creation time and set change time to NULL
- Uses errno checking to distinguish file not found errors from other stat() failures
- Performs filename validation through convert_and_check_filename() for security
- Memory management includes proper cleanup with pfree() for filename buffer
- The function signature supports both 1-arg and 2-arg variants through PG_NARGS() checking