# lookup_rowtype_tupdesc_noerror

## Location
src/backend/utils/cache/typcache.c: 1850 - 1866

## Overview
Public function to lookup a row type's tuple descriptor with optional error suppression, providing graceful handling when composite types may not exist.

## Definition


## Detailed Description
This function provides a variant of lookup_rowtype_tupdesc() with configurable error handling behavior. It serves as a wrapper around lookup_rowtype_tupdesc_internal() while adding:

1. **Conditional Error Handling**: When noError is true, returns NULL instead of throwing an error if the composite type is not found, allowing callers to handle missing types gracefully.

2. **Reference Counting**: Like its sibling function, it increments the reference count of successfully retrieved tuple descriptors via PinTupleDesc(), ensuring proper memory management.

3. **Selective Error Suppression**: Only suppresses errors for missing composite types; invalid type IDs will still generate errors regardless of the noError parameter.

The function is particularly useful in scenarios where the existence of a composite type is uncertain and the caller needs to implement fallback behavior rather than abort the operation.

## Parameters / Member Variables
- : The OID of the composite type to look up
- : Type modifier for transient record types (ignored for named composite types)  
- : If true, returns NULL instead of throwing an error when the composite type is not found

## Dependencies
- Functions called/Symbols referenced:
  - lookup_rowtype_tupdesc_internal
  - PinTupleDesc
- Called from (representative examples):
  - lookup_rowtype_tupdesc_domain

## Notes and Other Information
- This is a public API function that provides error-tolerant tuple descriptor lookup
- Callers must call ReleaseTupleDesc() when finished with non-NULL return values
- Bogus type IDs will still generate errors even when noError is true
- The noError parameter only affects composite type existence checks, not general validation
- Used when composite type existence is uncertain and graceful degradation is preferred
- Part of the type cache system's flexible error handling interface