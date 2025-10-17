# pg_control_system

## Location
[src/backend/utils/misc/pg_controldata.c:32-69](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/pg_controldata.c#L32-L69)

## Overview
A PostgreSQL SQL function that retrieves and returns basic system information from the control file as a composite tuple.

## Definition

```c
Datum
pg_control_system(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL SQL-callable function that reads the control file and returns essential system-level information as a row type. It acquires the ControlFileLock in shared mode to safely read the control file, validates the CRC checksum, and extracts four key system identifiers. The function is part of PostgreSQL's administrative interface that allows SQL queries to access control file metadata without requiring direct file system access.

## Parameters / Member Variables
- Returns a composite tuple containing:
  - : Version number of the control file format
  - : System catalog version number  
  - : Unique system identifier for the database cluster
  - : Timestamp when the control file was last updated

## Dependencies
- Functions called/Symbols referenced:
  - [get_call_result_type](../g/get_call_result_type.md): Validates return type
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease: Manages concurrent access to control file
  - [get_controlfile](../g/get_controlfile.md): Reads and parses the control file
  - [Int32GetDatum](../I/Int32GetDatum.md)/Int64GetDatum: Converts values to PostgreSQL Datum format
  - [time_t_to_timestamptz](../t/time_t_to_timestamptz.md): Converts time_t to PostgreSQL timestamp
  - [heap_form_tuple](../h/heap_form_tuple.md): Creates the return tuple
  - [ControlFileData](../C/ControlFileData.md): Structure containing control file data
- Called from (representative examples):
  - SQL queries via function call mechanism

## Notes and Other Information
- Requires shared lock on ControlFileLock to ensure consistent reads
- Validates control file CRC checksum and raises ERROR if corrupted
- Part of the pg_controldata family of functions for administrative access
- Located in src/backend/utils/misc/pg_controldata.c:32-69

## Simplified Source

```c
Datum
pg_control_system(PG_FUNCTION_ARGS)
{
    Datum values[4];
    bool nulls[4];
    TupleDesc tupdesc;

    // Validate function return type is composite
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");

    // Read control file with shared lock
    LWLockAcquire(ControlFileLock, LW_SHARED);
    bool crc_ok;
    ControlFileData *ControlFile = get_controlfile(DataDir, &crc_ok);
    LWLockRelease(ControlFileLock);

    // Verify control file integrity
    if (!crc_ok)
        ereport(ERROR, (errmsg("calculated CRC checksum does not match value stored in file")));

    // Extract system information into return values
    values[0] = Int32GetDatum(ControlFile->pg_control_version);  // Control file version
    values[1] = Int32GetDatum(ControlFile->catalog_version_no);  // Catalog version
    values[2] = Int64GetDatum(ControlFile->system_identifier);   // System identifier
    values[3] = TimestampTzGetDatum(time_t_to_timestamptz(ControlFile->time)); // Timestamp

    // All values are non-null
    nulls[0] = nulls[1] = nulls[2] = nulls[3] = false;

    // Create and return composite tuple
    HeapTuple htup = heap_form_tuple(tupdesc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(htup));
}
```