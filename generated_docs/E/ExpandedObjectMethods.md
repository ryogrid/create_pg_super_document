# ExpandedObjectMethods

## Location
[src/include/utils/expandeddatum.h:74-78](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/expandeddatum.h#L74-L78)

## Overview
A struct containing function pointers that define the required methods for managing expanded object types in PostgreSQL, specifically for converting between expanded and flattened representations.

## Definition
```c
typedef struct ExpandedObjectMethods
{
    EOM_get_flat_size_method get_flat_size;
    EOM_flatten_into_method flatten_into;
} ExpandedObjectMethods;
```

## Detailed Description
The `ExpandedObjectMethods` struct defines the interface that all expanded object types must implement to support conversion between their expanded (in-memory optimized) and flattened (on-disk storage) representations. This is part of PostgreSQL's expanded datum system, which provides memory-optimized representations of complex data types like arrays and records.

The struct contains two function pointers that handle the core operations needed for expanded object management:
1. Computing the size required for the flattened representation
2. Actually constructing the flattened representation in caller-allocated memory

These methods are called when PostgreSQL needs to serialize an expanded object back to its on-disk format, such as when storing values in tuples or preparing data for transmission.

## Member Variables
- `get_flat_size`: Function pointer of type `EOM_get_flat_size_method` that computes the total space needed for the flattened representation, including header. The signature is `Size (*EOM_get_flat_size_method)(ExpandedObjectHeader *eohptr)`.
- `flatten_into`: Function pointer of type `EOM_flatten_into_method` that constructs the flattened representation in caller-allocated space. The signature is `void (*EOM_flatten_into_method)(ExpandedObjectHeader *eohptr, void *result, Size allocated_size)`.

## Dependencies
- Functions called/Symbols referenced:
  - EOM_get_flat_size_method (typedef)
  - EOM_flatten_into_method (typedef)
- Used by (representative examples):
  - [ExpandedObjectHeader](ExpandedObjectHeader.md) (as eoh_methods member)
  - [EOH_init_header](EOH_init_header.md)
  - MakeExpandedObjectReadOnly

## Notes and Other Information
- The flattened representation must be a valid in-line, non-compressed, 4-byte-header varlena object
- The `get_flat_size` method may be called multiple times during heap tuple construction, so it should be optimized to avoid excessive overhead
- The `allocated_size` parameter in `flatten_into` is always the result of a preceding `get_flat_size` call and is provided for cross-checking
- This struct is embedded in `ExpandedObjectHeader` to provide type-specific behavior for different expanded object implementations