# toast_fetch_datum

## Location
[src/backend/access/common/detoast.c:343-395](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/detoast.c#L343-L395)

## Overview
Reconstructs an in-memory Datum from chunks stored in a TOAST relation, handling the complete retrieval of externally stored data that was too large to store directly in the main table.

## Definition


## Detailed Description
This function is responsible for reconstructing large variable-length data (Datum) that has been stored out-of-line in a TOAST (The Oversized-Attribute Storage Technique) table. When PostgreSQL encounters data that is too large to store directly in a table page, it stores the data in chunks in a separate TOAST table and leaves a pointer in the original location. This function takes that pointer and reconstructs the original data by fetching all the chunks from the TOAST table and reassembling them into a contiguous memory structure.

The function handles both compressed and uncompressed external data, properly setting the appropriate headers and allocating the correct amount of memory for the reconstructed datum. It ensures data integrity by validating that the input is indeed an external on-disk datum before proceeding with the reconstruction.

## Parameters / Member Variables
- : Pointer to a varlena structure containing the TOAST pointer that references the externally stored data chunks

## Dependencies
- Functions called/Symbols referenced:
  - VARATT_IS_EXTERNAL_ONDISK
  - VARATT_EXTERNAL_GET_POINTER
  - VARATT_EXTERNAL_GET_EXTSIZE
  - VARATT_EXTERNAL_IS_COMPRESSED
  - SET_VARSIZE_COMPRESSED
  - SET_VARSIZE
  - table_open
  - [table_relation_fetch_toast_slice](table_relation_fetch_toast_slice.md)
  - table_close
  - [palloc](../p/palloc.md)
  - elog
- Called from:
  - [detoast_external_attr](../d/detoast_external_attr.md)
  - [detoast_attr](../d/detoast_attr.md)
  - [detoast_attr_slice](../d/detoast_attr_slice.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the detoast.c compilation unit
- The function includes error checking to ensure it's only called for on-disk external datums
- Memory allocation is performed using PostgreSQL's palloc, which integrates with the database's memory context system
- The function properly handles both compressed and uncompressed external data by setting appropriate VARSIZE headers
- TOAST table access is performed with AccessShareLock to ensure data consistency during retrieval
- The function handles edge cases such as zero-length attributes gracefully