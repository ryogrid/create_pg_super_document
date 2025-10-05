# enum_recv

## Location
[src/backend/utils/adt/enum.c:179-220](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/enum.c#L179-L220)

## Overview
Converts binary protocol representation of enum values to internal OID format for PostgreSQL's binary I/O operations.

## Definition

```c
Datum
enum_recv(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the binary input conversion for PostgreSQL enum types, handling enum values received through the binary protocol (such as in COPY BINARY operations or prepared statement parameters). It extracts the enum label from the binary message buffer and converts it to the corresponding internal OID representation.

The function performs similar validation to enum_in but works with binary protocol data instead of C strings. It extracts the text representation from the message buffer, validates the string length, looks up the enum value in the system catalog, and ensures the value is safe to use (not uncommitted). The binary protocol allows for more efficient data transfer while maintaining the same safety guarantees as text input.

## Parameters / Member Variables
-  (PG_GETARG_POINTER(0)): StringInfo buffer containing the binary protocol data
-  (PG_GETARG_OID(1)): The OID of the enum type that this value should belong to

## Dependencies
- Functions called/Symbols referenced:
  - [pq_getmsgtext](../p/pq_getmsgtext.md)
  - NAMEDATALEN
  - [SearchSysCache2](../S/SearchSysCache2.md)
  - [CStringGetDatum](../C/CStringGetDatum.md)
  - [check_safe_enum_use](../c/check_safe_enum_use.md)
  - Form_pg_enum
  - PG_RETURN_OID
- Called from (representative examples):
  - No direct references found (called via function manager for binary protocol)

## Notes and Other Information
- This is part of PostgreSQL's binary I/O support system, complementing enum_in for text input
- Uses pq_getmsgtext to extract string data from the binary protocol buffer
- Performs the same safety validations as enum_in, including length checks and uncommitted value detection
- Properly manages memory by calling pfree on the extracted name string
- The binary protocol provides more efficient data transfer compared to text representation
- Essential for COPY BINARY operations and prepared statements with binary parameter formats
- Maintains the same error reporting patterns as the text input function for consistency

## Simplified Source

```c
Datum enum_recv(PG_FUNCTION_ARGS) {
    StringInfo buffer = (StringInfo) PG_GETARG_POINTER(0);
    Oid enum_type_oid = PG_GETARG_OID(1);
    int name_length;

    // Extract enum name from binary protocol buffer
    char *enum_name = pq_getmsgtext(buffer, buffer->len - buffer->cursor, &name_length);

    // Validate input length to prevent cache lookup failures
    if (strlen(enum_name) >= NAMEDATALEN) {
        ereport(ERROR, "invalid input value for enum: name too long");
    }

    // Look up the enum value in system catalog
    HeapTuple enum_tuple = SearchSysCache2(ENUMTYPOIDNAME,
                                          ObjectIdGetDatum(enum_type_oid),
                                          CStringGetDatum(enum_name));

    if (!HeapTupleIsValid(enum_tuple)) {
        ereport(ERROR, "invalid input value for enum: value not found");
    }

    // Ensure the enum value is safe to use (committed transaction)
    check_safe_enum_use(enum_tuple);

    // Extract the OID of the enum value
    Oid enum_value_oid = ((Form_pg_enum) GETSTRUCT(enum_tuple))->oid;

    ReleaseSysCache(enum_tuple);
    pfree(enum_name);  // Clean up allocated memory

    PG_RETURN_OID(enum_value_oid);
}
```