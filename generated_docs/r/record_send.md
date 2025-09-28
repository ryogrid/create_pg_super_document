# record_send

## Location
[src/backend/utils/adt/rowtypes.c:687-822](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rowtypes.c#L687-L822)

## Overview
Converts PostgreSQL's internal binary representation of a composite type (record) into binary format for network transmission or storage.

## Definition

```c
structure */
	tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
```
## Detailed Description
The  function serves as the binary output conversion function for any composite type in PostgreSQL. It takes a  (the internal binary format) and converts it into a standardized binary format suitable for network transmission or external storage. This function is part of PostgreSQL's binary I/O protocol, generating binary data that can be consumed by  or external applications.

The function extracts type information from the tuple header, decomposes the tuple into individual column values, and formats each value using the appropriate type-specific binary send function. It uses PostgreSQL's message buffer protocol to write the column count, individual column type OIDs, data lengths, and binary column data. The output format includes metadata that enables proper reconstruction of the record.

## Parameters / Member Variables
- : Input  containing the binary representation of the record to be converted

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md): Stack overflow protection for recursive calls
  - HeapTupleHeaderGetTypeId: Extracts type OID from tuple header
  - HeapTupleHeaderGetTypMod: Extracts type modifier from tuple header
  - [lookup_rowtype_tupdesc](../l/lookup_rowtype_tupdesc.md): Retrieves tuple descriptor for the record type
  - [heap_deform_tuple](../h/heap_deform_tuple.md): Extracts individual column values from tuple
  - [pq_begintypsend](../p/pq_begintypsend.md): Initializes binary output buffer
  - [pq_sendint32](../p/pq_sendint32.md): Sends 32-bit integers to binary buffer
  - [pq_sendbytes](../p/pq_sendbytes.md): Sends byte data to binary buffer
  - [pq_endtypsend](../p/pq_endtypsend.md): Finalizes binary output buffer
  - [getTypeBinaryOutputInfo](../g/getTypeBinaryOutputInfo.md): Gets binary output function info for column types
  - [SendFunctionCall](../S/SendFunctionCall.md): Calls type-specific binary send functions
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md): Memory allocation in function context
  - ReleaseTupleDesc: Releases tuple descriptor reference

- Called from (representative examples):
  - Type system as registered binary send function for composite types
  - Binary protocol handlers when transmitting composite type data
  - External data export utilities requiring binary format

## Notes and Other Information
- Generates binary output compatible with PostgreSQL's binary protocol specification
- Includes column count metadata to enable proper parsing by receive functions
- Embeds type OID for each column to support type validation during input
- Uses -1 length encoding to represent null values in the binary stream
- Handles dropped columns by excluding them from the output and adjusting column counts
- Uses function-local caching (fn_extra) to optimize repeated calls with same type
- Part of PostgreSQL's binary protocol for efficient data transfer
- Output format is platform-independent and suitable for network transmission
- Memory management uses palloc/pfree for PostgreSQL compatibility

## Simplified Source

```c
// Simplified version of record_send
Datum record_send(PG_FUNCTION_ARGS) {
    HeapTupleHeader rec = PG_GETARG_HEAPTUPLEHEADER(0);

    // Extract type information from tuple header
    Oid tupType = HeapTupleHeaderGetTypeId(rec);
    int32 tupTypmod = HeapTupleHeaderGetTypMod(rec);
    TupleDesc tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
    int ncolumns = tupdesc->natts;

    // Build temporary tuple structure for processing
    HeapTupleData tuple;
    setup_temp_tuple(&tuple, rec);

    // Setup or reuse cached I/O information
    RecordIOData *my_extra = setup_record_io_cache(fcinfo, ncolumns, tupType, tupTypmod);

    // Extract column values from tuple
    Datum *values = (Datum *) palloc(ncolumns * sizeof(Datum));
    bool *nulls = (bool *) palloc(ncolumns * sizeof(bool));
    heap_deform_tuple(&tuple, tupdesc, values, nulls);

    // Initialize binary output buffer
    StringInfoData buf;
    pq_begintypsend(&buf);

    // Write column count (excluding dropped columns)
    int validcols = count_valid_columns(tupdesc, ncolumns);
    pq_sendint32(&buf, validcols);

    // Process each column
    for (int i = 0; i < ncolumns; i++) {
        Form_pg_attribute att = TupleDescAttr(tupdesc, i);

        // Skip dropped columns
        if (att->attisdropped)
            continue;

        // Write column type OID
        pq_sendint32(&buf, att->atttypid);

        // Handle NULL values
        if (nulls[i]) {
            pq_sendint32(&buf, -1);  // -1 indicates NULL
            continue;
        }

        // Convert column value to binary and send
        setup_column_send_function(my_extra, i, att->atttypid, fcinfo);
        bytea *outputbytes = SendFunctionCall(&my_extra->columns[i].proc, values[i]);

        // Send data length and data
        int datalen = VARSIZE(outputbytes) - VARHDRSZ;
        pq_sendint32(&buf, datalen);
        pq_sendbytes(&buf, VARDATA(outputbytes), datalen);
    }

    // Cleanup and return binary result
    cleanup_resources(values, nulls, tupdesc);
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}
```

Key simplifications made:
- Extracted helper functions for common operations (setup_temp_tuple, setup_record_io_cache, etc.)
- Simplified the tuple setup and column processing logic
- Consolidated binary output operations
- Abstracted complex memory context and caching details
- Focused on the main binary serialization flow while preserving data integrity