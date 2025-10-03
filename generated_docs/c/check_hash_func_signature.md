# check_hash_func_signature

## Location
[src/backend/access/hash/hashvalidate.c:275-351](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashvalidate.c#L275-L351)

## Overview
The check_hash_func_signature function is a static helper function that validates hash function signatures specifically for PostgreSQL hash operator classes, including custom validation rules to accommodate built-in hash opclass implementation quirks.

## Definition
```c
static bool check_hash_func_signature(Oid funcid, int16 amprocnum, Oid argtype)
```

## Detailed Description
This function provides custom signature validation for hash access method support functions, implementing special validation logic beyond the standard amproc signature checking. It handles two types of hash functions:

1. **Standard Hash Functions (HASHSTANDARD_PROC)**: Expected to return int4 (32-bit hash) and take 1 argument
2. **Extended Hash Functions (HASHEXTENDED_PROC)**: Expected to return int8 (64-bit hash) and take 2 arguments (data + 64-bit salt)

The function implements special compatibility rules for built-in PostgreSQL hash operator classes that use "compatible but different" data types. These exceptions allow certain hash functions to be used with related data types that are physically compatible but not formally binary coercible, such as using hashint4() for dates, XIDs, and command IDs.

## Parameters / Member Variables
- `funcid`: The OID of the hash function to validate
- `amprocnum`: The support procedure number (HASHSTANDARD_PROC or HASHEXTENDED_PROC) 
- `argtype`: The expected argument data type for the hash function

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [IsBinaryCoercible](../I/IsBinaryCoercible.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - GETSTRUCT
- Called from (representative examples):
  - [hashvalidate](../h/hashvalidate.md) (during operator class validation)

## Simplified Source
```c
static bool check_hash_func_signature(Oid funcid, int16 amprocnum, Oid argtype) {
    bool result = true;
    Oid expected_rettype;
    int16 expected_nargs;

    // Determine expected signature based on procedure number
    switch (amprocnum) {
        case HASHSTANDARD_PROC:
            expected_rettype = INT4OID;  // 32-bit hash
            expected_nargs = 1;
            break;
        case HASHEXTENDED_PROC:
            expected_rettype = INT8OID;  // 64-bit hash
            expected_nargs = 2;
            break;
        default:
            elog(ERROR, "invalid amprocnum");
    }

    // Get function info from system catalog
    HeapTuple tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    Form_pg_proc procform = (Form_pg_proc) GETSTRUCT(tp);

    // Check basic signature
    if (procform->prorettype != expected_rettype ||
        procform->proretset ||
        procform->pronargs != expected_nargs) {
        result = false;
    }

    // Check argument type compatibility
    if (!IsBinaryCoercible(argtype, procform->proargtypes.values[0])) {
        // Special exceptions for built-in hash opclasses
        if ((funcid == F_HASHINT4 || funcid == F_HASHINT4EXTENDED) &&
            (argtype == DATEOID || argtype == XIDOID || argtype == CIDOID)) {
            // hashint4() allowed for dates, XIDs, CIDs
        } else if ((funcid == F_HASHINT8 || funcid == F_HASHINT8EXTENDED) &&
                   (argtype == XID8OID)) {
            // hashint8() allowed for XID8
        } else if ((funcid == F_TIMESTAMP_HASH || funcid == F_TIMESTAMP_HASH_EXTENDED) &&
                   argtype == TIMESTAMPTZOID) {
            // timestamp_hash() allowed for timestamptz
        } else if ((funcid == F_HASHCHAR || funcid == F_HASHCHAREXTENDED) &&
                   argtype == BOOLOID) {
            // hashchar() allowed for boolean
        } else if ((funcid == F_HASHVARLENA || funcid == F_HASHVARLENAEXTENDED) &&
                   argtype == BYTEAOID) {
            // hashvarlena() allowed for bytea
        } else {
            result = false;
        }
    }

    // Extended functions must have int8 salt as second argument
    if (expected_nargs == 2 && procform->proargtypes.values[1] != INT8OID) {
        result = false;
    }

    ReleaseSysCache(tp);
    return result;
}
```

## Notes and Other Information
- This is a custom implementation needed because standard amproc signature validation is too strict for PostgreSQL's built-in hash operator classes.
- The function includes hardcoded exceptions for specific function/type combinations that are known to be safe despite not passing formal coercibility checks.
- Extended hash functions must accept a 64-bit salt as their second parameter.
- The allowed exceptions include: hashint4() for dates/XIDs/CIDs, hashint8() for XID8, timestamp_hash() for timestamptz, hashchar() for boolean, and hashvarlena() for bytea.
- Located in src/backend/access/hash/hashvalidate.c:275-351.