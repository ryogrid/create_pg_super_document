# getmissingattr

## Location
src/backend/access/common/heaptuple.c: 147 - 214

## Overview
The `getmissingattr` function returns the missing value for a specified attribute from a tuple descriptor, with caching support for efficient repeated access to by-reference missing values.

## Definition
```c
Datum getmissingattr(TupleDesc tupleDesc, int attnum, bool *isnull)
```

## Detailed Description
This function retrieves the missing value for a table column (attribute) when that attribute has a defined missing value. It handles both by-value and by-reference data types, with an optimization that caches by-reference values to avoid repeated copying of large data. The function first checks if the specified attribute has a missing value defined, then either returns the cached value or creates a new cache entry. For by-value attributes, it returns the value directly without caching. If no missing value is defined, it sets the null flag and returns NULL.

## Parameters / Member Variables
- `tupleDesc`: Tuple descriptor containing attribute information and constraints
- `attnum`: 1-based attribute number to get the missing value for
- `isnull`: Output parameter set to true if the attribute should be null, false otherwise

## Dependencies
- Functions called/Symbols referenced:
  - `TupleDescAttr`: Macro to get attribute descriptor from tuple descriptor
  - `AttrMissing`: Structure containing missing value information
  - `missing_cache_key`: Cache key structure for missing value lookup
  - `init_missing_cache`: Initializes the missing value cache if not already done
  - `VARSIZE_ANY`: Macro to get size of variable-length data
  - `hash_search`: Searches or inserts entry in hash table
  - `HASH_ENTER`: Flag for hash search to enter if not found
  - `datumCopy`: Creates a copy of a Datum value
  - `MemoryContextSwitchTo`: Switches memory allocation context
  - `TopMemoryContext`: Long-lived memory context
  - `PointerGetDatum`: Converts pointer to Datum
- Called from (representative examples):
  - `heap_deform_tuple`: Uses this to get missing values during tuple deformation
  - `HeapTupleClearHeapOnly`: Macro that may need missing attribute values
  - `heap_getattr`: General attribute getter that handles missing values

## Notes and Other Information
- Returns a Datum value that represents the missing attribute value
- Implements caching optimization for by-reference (non-by-value) attributes to avoid repeated memory allocation
- By-value attributes are returned directly without caching since they are small and copying is cheap
- Uses lazy initialization of the missing value cache - cache is only created when first needed
- Cache entries are stored in TopMemoryContext to persist across transactions
- Handles both fixed-length and variable-length data types appropriately
- Part of PostgreSQL ADD COLUMN with DEFAULT optimization, allowing new columns to avoid rewriting existing data