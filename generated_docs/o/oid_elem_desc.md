# oid_elem_desc

## Location
[src/backend/access/rmgrdesc/rmgrdesc_utils.c:58-61](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/rmgrdesc_utils.c#L58-L61)

## Overview
A callback function that formats Oid (Object Identifier) values as unsigned integers for use with the array_desc utility function.

## Definition

```c
void
oid_elem_desc(StringInfo buf, void *relid, void *data)
```
## Detailed Description
The  function is a specialized element description callback designed to work with the  utility function. It formats Oid values (PostgreSQL's Object Identifier type) as unsigned integers in the output buffer. This function is commonly used when describing arrays of relation OIDs or other object identifiers in WAL record descriptions.

The function takes a void pointer to an Oid value, casts it appropriately, dereferences it, and formats it as an unsigned integer using . Oids are fundamental identifiers in PostgreSQL used to uniquely identify database objects like tables, indexes, and types.

## Parameters / Member Variables
- : StringInfo buffer where the formatted OID will be appended
- : Pointer to the Oid value to be formatted (cast from void*)
- : Additional data parameter (unused in this implementation but required by callback signature)

## Dependencies
- Functions called/Symbols referenced:
  - [appendStringInfo](../a/appendStringInfo.md)
  - Oid (data type)
- Called from (representative examples):
  - [heap_desc](../h/heap_desc.md)

## Notes and Other Information
- This is a callback function specifically designed for use with array_desc
- Oid is PostgreSQL's Object Identifier type, a fundamental data type for uniquely identifying database objects
- The parameter name 'relid' suggests it's commonly used for relation (table/index) identifiers
- The function follows the standard element description callback signature
- The data parameter is not used but must be present to match the expected callback interface
- Used in heap WAL record descriptions when arrays of relation OIDs need to be displayed