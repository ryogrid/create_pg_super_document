# ExpandedObjectHeader

## Location
src/include/varatt.h: 72 - 73

## Overview
A fundamental header structure that must be contained in every expanded object, providing essential infrastructure for PostgreSQL's expanded datum system.

## Definition


## Detailed Description
The ExpandedObjectHeader structure serves as the mandatory foundation for PostgreSQL's expanded datum system, which provides an efficient way to handle large, complex data structures that can be expensive to repeatedly decompress and process. This header is typically embedded within larger, type-specific structures that add additional fields relevant to the particular data type.

The design philosophy centers around memory management efficiency and flexibility. All data associated with an expanded object, including the header and subsidiary data, are stored within the eoh_context memory context. This allows for simple cleanup by deleting the context and enables flexible storage lifespan management through context reparenting.

A key feature is the provision of two standard TOAST pointers within the header - one read-write and one read-only. This dual-pointer approach allows functions to return either type of pointer without additional memory allocation and without concerns about the lifespan of separately allocated objects.

## Parameters / Member Variables
- : A phony varlena header that always contains EOH_HEADER_MAGIC, serving as a type identifier
- : Pointer to the ExpandedObjectMethods structure containing type-specific function pointers for operations on this object type
- : Memory context that contains this header and all associated subsidiary data, enabling efficient memory management
- : Pre-allocated read-write TOAST pointer for this object, sized according to EXPANDED_POINTER_SIZE
- : Pre-allocated read-only TOAST pointer for this object, sized according to EXPANDED_POINTER_SIZE

## Dependencies
- Functions called/Symbols referenced:
  - ExpandedObjectMethods
  - EXPANDED_POINTER_SIZE
  - MemoryContext
  - int32
- Called from (representative examples):
  - detoast_external_attr
  - EA_get_flat_size
  - EA_flatten_into
  - datumCopy
  - datumSerialize
  - EOH_init_header
  - MakeExpandedObjectReadOnlyInternal
  - TransferExpandedObject

## Notes and Other Information
- Must be embedded in larger type-specific structures rather than used standalone
- All memory management occurs through the eoh_context for simplified cleanup
- The dual TOAST pointer design eliminates need for additional allocations in common scenarios
- EOH_HEADER_MAGIC serves as a type identification mechanism
- Objects can own additional resources beyond the memory context through reset callbacks
- Central to PostgreSQL's strategy for efficient handling of complex, large data structures
- Enables lazy evaluation and in-place modification of complex data types