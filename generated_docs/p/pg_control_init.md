# pg_control_init

## Location
[src/backend/utils/misc/pg_controldata.c:204-260](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/pg_controldata.c#L204-L260)

## Overview
A PostgreSQL SQL function that retrieves initialization-time configuration parameters from the control file, providing access to compile-time and cluster-initialization settings.

## Definition

```c
Datum
pg_control_init(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function extracts and returns fundamental configuration parameters that were set when the PostgreSQL cluster was initialized. These parameters represent compile-time constants and initialization-time settings that define the basic structural characteristics of the database cluster. The function provides essential information about data layout, block sizes, limits, and other architectural parameters that cannot be changed after cluster initialization. This information is crucial for compatibility checking, performance analysis, and understanding cluster characteristics.

## Parameters / Member Variables
- Returns a composite tuple containing 11 fields:
  - : Maximum alignment required for data types
  - : Database block size in bytes
  - : Maximum size of relation segment files
  - : WAL block size in bytes
  - : WAL segment size in bytes  
  - : Maximum length for object names
  - : Maximum number of keys per index
  - : Maximum size of TOAST chunks
  - : Chunk size for large objects
  - : Whether 8-byte floats are passed by value
  - : Version of data page checksum algorithm

## Dependencies
- Functions called/Symbols referenced:
  - [get_call_result_type](../g/get_call_result_type.md): Validates return type
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease: Manages concurrent access to control file
  - [get_controlfile](../g/get_controlfile.md): Reads and parses the control file
  - [Int32GetDatum](../I/Int32GetDatum.md): Converts integer values to PostgreSQL Datum format
  - [BoolGetDatum](../B/BoolGetDatum.md): Converts boolean values to Datum format
  - [heap_form_tuple](../h/heap_form_tuple.md): Creates the return tuple
  - [ControlFileData](../C/ControlFileData.md): Structure containing control file data
- Called from (representative examples):
  - SQL queries via function call mechanism

## Notes and Other Information
- Requires shared lock on ControlFileLock to ensure consistent reads
- Validates control file CRC checksum and raises ERROR if corrupted
- Returns immutable cluster configuration set at initdb time
- Critical for version compatibility and architectural understanding
- Block sizes and limits affect performance and storage characteristics
- These values cannot be changed without reinitializing the cluster
- Used for compatibility checking between different PostgreSQL installations
- Part of the administrative interface for cluster configuration inspection
- Located in src/backend/utils/misc/pg_controldata.c:204-260

## Simplified Source

```c
Datum
pg_control_init(PG_FUNCTION_ARGS)
{
    Datum values[11];
    bool nulls[11];
    TupleDesc tupdesc;
    HeapTuple htup;
    ControlFileData *ControlFile;
    bool crc_ok;

    // Validate return type is composite
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");

    // Read control file with proper locking
    LWLockAcquire(ControlFileLock, LW_SHARED);
    ControlFile = get_controlfile(DataDir, &crc_ok);
    LWLockRelease(ControlFileLock);

    // Validate control file integrity
    if (!crc_ok)
        ereport(ERROR, (errmsg("calculated CRC checksum does not match value stored in file")));

    // Extract configuration values from control file
    values[0] = Int32GetDatum(ControlFile->maxAlign);
    values[1] = Int32GetDatum(ControlFile->blcksz);
    values[2] = Int32GetDatum(ControlFile->relseg_size);
    values[3] = Int32GetDatum(ControlFile->xlog_blcksz);
    values[4] = Int32GetDatum(ControlFile->xlog_seg_size);
    values[5] = Int32GetDatum(ControlFile->nameDataLen);
    values[6] = Int32GetDatum(ControlFile->indexMaxKeys);
    values[7] = Int32GetDatum(ControlFile->toast_max_chunk_size);
    values[8] = Int32GetDatum(ControlFile->loblksize);
    values[9] = BoolGetDatum(ControlFile->float8ByVal);
    values[10] = Int32GetDatum(ControlFile->data_checksum_version);

    // Mark all values as non-null
    for (int i = 0; i < 11; i++)
        nulls[i] = false;

    // Create and return the tuple
    htup = heap_form_tuple(tupdesc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(htup));
}
```