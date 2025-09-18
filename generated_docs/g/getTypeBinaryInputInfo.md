# getTypeBinaryInputInfo

## Location
src/backend/utils/cache/lsyscache.c: 2940 - 2972

## Overview
Retrieves information needed for binary input of values for a given PostgreSQL data type, specifically the binary receive function OID and the type I/O parameter.

## Definition
```c
void getTypeBinaryInputInfo(Oid type, Oid *typReceive, Oid *typIOParam)
```

## Detailed Description
This function looks up binary input information for a specified type from the PostgreSQL system catalog. It searches the pg_type system table to find the type's binary receive function (typreceive) and determines the appropriate I/O parameter to pass to that function. The function performs validation to ensure the type is properly defined and has a binary input function available.

The function is part of PostgreSQL's type system infrastructure and is used when the system needs to deserialize binary-format data into internal PostgreSQL values. This is commonly used in COPY operations, logical replication, and protocol-level binary data handling.

## Parameters / Member Variables
- `type`: The OID of the PostgreSQL data type for which binary input information is needed
- `typReceive`: Output parameter that receives the OID of the type's binary receive function (from pg_type.typreceive)
- `typIOParam`: Output parameter that receives the type I/O parameter (determined by getTypeIOParam logic)

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1 (system catalog cache lookup)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (extract struct from heap tuple)
  - Form_pg_type (pg_type catalog structure)
  - getTypeIOParam (determine I/O parameter for the type)
  - ReleaseSysCache (release cache reference)
  - format_type_be (error message formatting)
- Called from (representative examples):
  - BeginCopyFrom (COPY command binary input setup)
  - slot_store_data (logical replication worker)
  - exec_bind_message (protocol binary parameter binding)
  - record_recv (composite type binary input)

## Notes and Other Information
- The function will throw an ERROR if the type OID is invalid or not found in the system catalog
- It validates that the type is fully defined (not just a shell type) before proceeding
- If no binary receive function is available for the type, it raises an error with ERRCODE_UNDEFINED_FUNCTION
- The function is essential for PostgreSQL's binary I/O operations and protocol support
- Part of the larger family of type information lookup functions in lsyscache.c