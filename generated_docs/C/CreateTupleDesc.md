# CreateTupleDesc

## Location
[src/backend/access/common/tupdesc.c:112-132](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tupdesc.c#L112-L132)

## Overview
Creates a complete tuple descriptor by copying attribute definitions from a given Form_pg_attribute array into a new TupleDesc structure.

## Definition
```c
TupleDesc CreateTupleDesc(int natts, Form_pg_attribute *attrs)
```

## Detailed Description
This function creates a fully populated tuple descriptor by first creating a template using CreateTemplateTupleDesc, then copying the provided attribute definitions into it. It performs a memcpy of each attribute's fixed-size portion (ATTRIBUTE_FIXED_PART_SIZE bytes) from the source array to the new tuple descriptor. This function is essential for creating tuple descriptors when you have a complete set of attribute definitions ready to copy. The resulting tuple descriptor initially has anonymous record type information which can be overwritten by the caller if needed.

## Parameters / Member Variables
- `natts`: The number of attributes in the attrs array
- `attrs`: Array of pointers to Form_pg_attribute structures containing the attribute definitions to copy

## Dependencies
- Functions called/Symbols referenced:
  - [CreateTemplateTupleDesc](CreateTemplateTupleDesc.md)
  - memcpy
  - TupleDescAttr (macro for accessing attributes)
  - ATTRIBUTE_FIXED_PART_SIZE (constant)
- Called from (representative examples):
  - [InsertOneTuple](../I/InsertOneTuple.md)
  - [AddNewAttributeTuples](../A/AddNewAttributeTuples.md)
  - TupleDescAttr (header macro)

## Notes and Other Information
- Builds upon CreateTemplateTupleDesc to create the base structure
- Only copies ATTRIBUTE_FIXED_PART_SIZE bytes of each attribute, which excludes variable-length trailing data
- The attrs parameter is an array of pointers to Form_pg_attribute structures, not the structures themselves
- Initializes with anonymous record type (RECORDOID) that caller can override
- Used primarily in bootstrap and catalog operations where complete attribute definitions are available
- More efficient than manually setting up each attribute individually when you have a complete attribute array
- The copied attributes retain their original metadata including names, types, and other properties