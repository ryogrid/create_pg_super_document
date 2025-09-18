# heap_getattr

## Location
src/include/access/htup_details.h: 797 - 811

## Overview
`heap_getattr` is the primary interface function for extracting attributes from heap tuples, handling both user and system attributes with proper bounds checking.

## Definition
```c
static inline Datum
heap_getattr(HeapTuple tup, int attnum, TupleDesc tupleDesc, bool *isnull)
```

## Detailed Description
`heap_getattr` serves as the main entry point for attribute extraction from heap tuples in PostgreSQL. It provides a safe, comprehensive interface that can handle:

1. **User attributes** (attnum > 0): Routes to appropriate specialized functions based on attribute availability
2. **System attributes** (attnum <= 0): Delegates to `heap_getsysattr` for system column access
3. **Missing attributes**: Returns default values for attributes that don't exist in the tuple but are defined in newer schema versions

The function implements a three-way dispatch mechanism:
- For missing user attributes (beyond tuple's column count), it calls `getmissingattr` to provide default values
- For existing user attributes, it uses the optimized `fastgetattr` path
- For system attributes, it delegates to the specialized `heap_getsysattr` function

This design ensures compatibility with schema evolution while maintaining performance for common cases.

## Parameters / Member Variables
- `tup`: Pointer to the heap tuple from which to extract the attribute
- `attnum`: Attribute number (1-based for user attributes, 0 or negative for system attributes)
- `tupleDesc`: Tuple descriptor containing schema information and attribute metadata
- `isnull`: Output parameter indicating whether the returned value is NULL

## Dependencies
- Functions called/Symbols referenced:
  - `HeapTupleHeaderGetNatts` - macro to get the number of attributes in the tuple header
  - [getmissingattr](../g/getmissingattr.md) - function to retrieve missing attribute default values
  - [fastgetattr](../f/fastgetattr.md) - optimized function for existing user attribute retrieval
  - [heap_getsysattr](heap_getsysattr.md) - specialized function for system attribute access
- Called from (representative examples):
  - `SPI_getbinval` - SPI interface for binary value retrieval
  - [GetAttributeByName](../G/GetAttributeByName.md) - utility function for named attribute access
  - [ExecEvalFieldSelect](../E/ExecEvalFieldSelect.md) - executor support for field selection
  - [CatalogCacheCreateEntry](../C/CatalogCacheCreateEntry.md) - catalog cache entry creation

## Notes and Other Information
- **Universal interface**: Unlike `fastgetattr`, this function can safely handle any attribute number including system attributes
- **Schema evolution support**: Handles cases where tuples have fewer attributes than the current table definition through `getmissingattr`
- **Performance optimization**: Still maintains efficiency by routing to `fastgetattr` for the common case of existing user attributes
- **Safety first**: Includes proper bounds checking and validation, making it the recommended choice when attribute validity is uncertain
- **NULL value handling**: Consistently sets `*isnull` output parameter across all code paths
- **Static inline implementation**: Provides function call elimination for performance while maintaining a clean interface