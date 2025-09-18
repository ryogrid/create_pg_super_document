# CreateTemplateTupleDesc

## Location
src/backend/access/common/tupdesc.c: 67 - 111

## Overview
Creates an empty tuple descriptor structure with a specified number of attributes, serving as a template for building complete tuple descriptors.

## Definition
```c
TupleDesc CreateTemplateTupleDesc(int natts)
```

## Detailed Description
This function allocates and initializes an empty tuple descriptor structure with space for the specified number of attributes. It serves as a foundational building block for creating tuple descriptors in PostgreSQL. The function allocates memory for both the TupleDescData structure and the attribute array in a single allocation. The tuple descriptor is initialized with default values: anonymous record type information (RECORDOID), no constraints, and a reference count of -1 (indicating it's not reference-counted initially). The actual attribute definitions must be filled in separately after calling this function.

## Parameters / Member Variables
- `natts`: The number of attributes (columns) that this tuple descriptor will contain (must be >= 0)

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation)
  - Assert (sanity checking)
  - [TupleDescData](../T/TupleDescData.md) (structure definition)
  - RECORDOID, ATTRIBUTE_FIXED_PART_SIZE, FormData_pg_attribute (constants/types)
- Called from (representative examples):
  - [CreateTupleDesc](CreateTupleDesc.md)
  - [CreateTupleDescCopy](CreateTupleDescCopy.md)
  - [CreateTupleDescCopyConstr](CreateTupleDescCopyConstr.md)
  - [BuildDescFromLists](../B/BuildDescFromLists.md)
  - [ConstructTupleDescriptor](ConstructTupleDescriptor.md)
  - [ExecTypeFromTLInternal](../E/ExecTypeFromTLInternal.md)
  - [AllocateRelationDesc](../A/AllocateRelationDesc.md)
  - [formrdesc](../f/formrdesc.md)

## Notes and Other Information
- Performs sanity check to ensure natts >= 0
- Allocates memory using palloc with careful size calculation including attribute array space
- Initializes tdtypeid to RECORDOID (anonymous record type) which can be overwritten by caller
- Sets tdtypmod to -1 (no type modifier)
- Sets tdrefcount to -1 (not reference-counted initially)
- Sets constr to NULL (no constraints initially)
- The attribute array uses FormData_pg_attribute elements, but only guarantees ATTRIBUTE_FIXED_PART_SIZE bytes are valid
- Memory layout optimization: allocates the descriptor and attribute array in a single palloc call
- This is a fundamental function used throughout PostgreSQL for creating tuple descriptors in various contexts