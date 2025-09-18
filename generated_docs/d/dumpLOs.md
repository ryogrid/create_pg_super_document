# dumpLOs

## Location
src/bin/pg_dump/pg_dump.c: 3904 - 3949

## Overview
Dumps the data contents of large objects (LOBs) in PostgreSQL, reading them from the database and writing them to the archive output.

## Definition


## Detailed Description
The  function handles the dumping of large object data during a pg_dump operation. It takes a  structure containing an array of large object OIDs and iterates through each one, opening the large object, reading its contents in chunks, and writing the data to the archive. The function uses the PostgreSQL large object API (, , ) to access the binary data and outputs it through the archive's  interface.

The function logs progress information and handles errors that may occur during large object access, such as failure to open a large object or read errors during data transfer.

## Parameters / Member Variables
- : Archive pointer for output operations and database connection
- : Void pointer that should be cast to  containing large object information including:
  - : Number of large objects to dump
  - : Array of large object OIDs to process

## Dependencies
- Functions called/Symbols referenced:
  -  (cast from arg parameter)
  -  (get database connection)
  -  (logging)
  - , ,  (PostgreSQL large object API)
  - ,  (archive LO boundary markers)
  -  (write data to archive)
  -  (buffer size constant)
  -  (large object read mode constant)
- Called from (representative examples):
  -  (main dump dispatch function)

## Notes and Other Information
- Returns 1 on successful completion
- Uses a fixed buffer size () for reading large object data in chunks
- Handles large objects that may be larger than available memory by streaming the data
- Part of the pg_dump utility's infrastructure for backing up PostgreSQL databases
- Large objects are a PostgreSQL-specific feature for storing binary data outside of regular table storage