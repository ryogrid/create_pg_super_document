# getTypeBinaryOutputInfo

## Location
[src/backend/utils/cache/lsyscache.c:2973-3005](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L2973-L3005)

## Overview
Retrieves information needed for binary output of values for a given PostgreSQL data type, specifically the binary send function OID and whether the type is variable-length.

## Definition
```c
void getTypeBinaryOutputInfo(Oid type, Oid *typSend, bool *typIsVarlena)
```

## Detailed Description
This function looks up binary output information for a specified type from the PostgreSQL system catalog. It searches the pg_type system table to find the type's binary send function (typsend) and determines whether the type is variable-length (varlena). The function performs validation to ensure the type is properly defined and has a binary output function available.

The function is part of PostgreSQL's type system infrastructure and is used when the system needs to serialize internal PostgreSQL values into binary format. This is commonly used in COPY operations, query result transmission, and protocol-level binary data handling.

## Parameters / Member Variables
- `type`: The OID of the PostgreSQL data type for which binary output information is needed
- `typSend`: Output parameter that receives the OID of the type's binary send function (from pg_type.typsend)
- `typIsVarlena`: Output parameter that indicates whether the type is variable-length (true if not passed by value and has length -1)

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system catalog cache lookup)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (extract struct from heap tuple)
  - Form_pg_type (pg_type catalog structure)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (release cache reference)
  - [format_type_be](../f/format_type_be.md) (error message formatting)
- Called from (representative examples):
  - [printtup_prepare_info](../p/printtup_prepare_info.md) (query result preparation)
  - [DoCopyTo](../D/DoCopyTo.md) (COPY command binary output setup)
  - [SendFunctionResult](../S/SendFunctionResult.md) (function call result transmission)
  - [record_send](../r/record_send.md) (composite type binary output)

## Notes and Other Information
- The function will throw an ERROR if the type OID is invalid or not found in the system catalog
- It validates that the type is fully defined (not just a shell type) before proceeding
- If no binary send function is available for the type, it raises an error with ERRCODE_UNDEFINED_FUNCTION
- The typIsVarlena determination is based on two pg_type fields: typbyval (passed by value) and typlen (type length, -1 indicates variable length)
- [Variable](../V/Variable.md)-length types require special handling in binary serialization for length encoding
- Part of the larger family of type information lookup functions in lsyscache.c