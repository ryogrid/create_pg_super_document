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

## Simplified Source

```c
Datum pg_stat_file(PG_FUNCTION_ARGS) {
    text *filename_t = PG_GETARG_TEXT_PP(0);
    bool missing_ok = false;

    // Check for optional missing_ok argument
    if (PG_NARGS() == 2)
        missing_ok = PG_GETARG_BOOL(1);

    // Convert and validate filename
    char *filename = convert_and_check_filename(filename_t);

    // Get file statistics
    struct stat fst;
    if (stat(filename, &fst) < 0) {
        if (missing_ok && errno == ENOENT)
            PG_RETURN_NULL();
        else
            ereport(ERROR, (errcode_for_file_access(),
                errmsg("could not stat file \"%s\": %m", filename)));
    }

    // Create tuple descriptor for return record (6 fields)
    TupleDesc tupdesc = CreateTemplateTupleDesc(6);
    TupleDescInitEntry(tupdesc, 1, "size", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, 2, "access", TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(tupdesc, 3, "modification", TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(tupdesc, 4, "change", TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(tupdesc, 5, "creation", TIMESTAMPTZOID, -1, 0);
    TupleDescInitEntry(tupdesc, 6, "isdir", BOOLOID, -1, 0);
    BlessTupleDesc(tupdesc);

    // Populate return values
    Datum values[6];
    bool isnull[6];
    memset(isnull, false, sizeof(isnull));

    values[0] = Int64GetDatum((int64) fst.st_size);
    values[1] = TimestampTzGetDatum(time_t_to_timestamptz(fst.st_atime));
    values[2] = TimestampTzGetDatum(time_t_to_timestamptz(fst.st_mtime));

    // Platform-specific: Unix has change time, Windows has creation time
#if !defined(WIN32) && !defined(__CYGWIN__)
    values[3] = TimestampTzGetDatum(time_t_to_timestamptz(fst.st_ctime));
    isnull[4] = true;  // No creation time on Unix
#else
    isnull[3] = true;  // No change time on Windows
    values[4] = TimestampTzGetDatum(time_t_to_timestamptz(fst.st_ctime));
#endif
    values[5] = BoolGetDatum(S_ISDIR(fst.st_mode));

    // Create and return tuple
    HeapTuple tuple = heap_form_tuple(tupdesc, values, isnull);
    pfree(filename);

    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}
```