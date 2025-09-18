# index_open

## Location
src/backend/access/index/indexam.c: 133 - 151

## Overview
Opens an index relation by its object identifier (OID) and optionally acquires a lock on the index, with validation that the relation is indeed an index.

## Definition


## Detailed Description
The  function is a convenience routine specifically adapted for index scan operations. It opens an index relation using the provided OID and lock mode, then validates that the opened relation is actually an index. This function is a wrapper around  that adds index-specific validation.

The function will raise an error if the index does not exist or if the relation is not a valid index type. If  is not , the specified kind of lock is obtained on the index. Generally,  should only be used if the caller knows it already has some appropriate lock on the index.

## Parameters
- : The object identifier (OID) of the index relation to open
- : The type of lock to acquire on the index (use  if already holding an appropriate lock)

## Dependencies
- Functions called/Symbols referenced:
  - [relation_open](../r/relation_open.md)
  - [validate_relation_kind](../v/validate_relation_kind.md)
- Called from (representative examples):
  - [brin_summarize_range](../b/brin_summarize_range.md)
  - [toast_open_indexes](../t/toast_open_indexes.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [DefineIndex](../D/DefineIndex.md)
  - [ExecOpenIndices](../E/ExecOpenIndices.md)
  - [get_relation_info](../g/get_relation_info.md)

## Notes and Other Information
- This is a convenience function specifically designed for index operations
- Some callers may prefer to use  directly if they don't need the index-specific validation
- The function ensures type safety by validating that the opened relation is actually an index
- Error handling is built-in: the function will raise an error rather than return NULL if the index doesn't exist
- Located in src/backend/access/index/indexam.c:133-151