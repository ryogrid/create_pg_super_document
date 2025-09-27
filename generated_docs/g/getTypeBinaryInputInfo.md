# getTypeBinaryInputInfo

## Location
[src/backend/utils/cache/lsyscache.c:2940-2972](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L2940-L2972)

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
  - [SearchSysCache1](../S/SearchSysCache1.md) (system catalog cache lookup)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (extract struct from heap tuple)
  - Form_pg_type (pg_type catalog structure)
  - [getTypeIOParam](getTypeIOParam.md) (determine I/O parameter for the type)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (release cache reference)
  - [format_type_be](../f/format_type_be.md) (error message formatting)
- Called from (representative examples):
  - [BeginCopyFrom](../B/BeginCopyFrom.md) (COPY command binary input setup)
  - [slot_store_data](../s/slot_store_data.md) (logical replication worker)
  - [exec_bind_message](../e/exec_bind_message.md) (protocol binary parameter binding)
  - [record_recv](../r/record_recv.md) (composite type binary input)

## Notes and Other Information
- The function will throw an ERROR if the type OID is invalid or not found in the system catalog
- It validates that the type is fully defined (not just a shell type) before proceeding
- If no binary receive function is available for the type, it raises an error with ERRCODE_UNDEFINED_FUNCTION
- The function is essential for PostgreSQL's binary I/O operations and protocol support
- Part of the larger family of type information lookup functions in lsyscache.c

## Simplified Source

```c
// Simplified version of getTypeBinaryInputInfo
void getTypeBinaryInputInfo(Oid type, Oid *typReceive, Oid *typIOParam) {
    // Step 1: Look up the type in the system catalog
    HeapTuple typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type));
    if (!HeapTupleIsValid(typeTuple)) {
        elog(ERROR, "cache lookup failed for type %u", type);
    }

    // Step 2: Extract type information from the catalog entry
    Form_pg_type typeInfo = (Form_pg_type) GETSTRUCT(typeTuple);

    // Step 3: Validate the type is properly defined
    if (!typeInfo->typisdefined) {
        ereport(ERROR, "type is only a shell");
    }

    // Step 4: Ensure binary input function exists
    if (!OidIsValid(typeInfo->typreceive)) {
        ereport(ERROR, "no binary input function available for type");
    }

    // Step 5: Return the binary receive function and I/O parameter
    *typReceive = typeInfo->typreceive;
    *typIOParam = getTypeIOParam(typeTuple);

    // Step 6: Clean up catalog cache reference
    ReleaseSysCache(typeTuple);
}
```

Key simplifications made:
- Simplified error messages for clarity (removed detailed formatting)
- Combined variable declaration and assignment where appropriate
- Added step-by-step comments explaining the main logic flow
- Preserved all essential validation and error handling
- Maintained the core algorithm structure