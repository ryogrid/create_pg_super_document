# build_dummy_expanded_header

## Location
src/backend/utils/adt/expandedrecord.c: 1402 - 1493

## Overview
Constructs a temporary "dummy" expanded record header used for domain constraint validation without modifying the state of the main expanded record.

## Definition


## Detailed Description
This function creates a specialized dummy expanded record header that serves as a temporary workspace for domain constraint checking. The dummy header contains proposed field values that can be validated without affecting the main record's state. This approach ensures that constraint violations don't leave the main record in a corrupted state.

The function employs a lazy allocation strategy - creating the dummy header only when first needed, or when the field count changes. The dummy header shares metadata with the main record but maintains its own field value arrays. Importantly, it uses the short-term memory context to ensure any detoasted values created during constraint checking are automatically cleaned up.

## Parameters / Member Variables
- : Pointer to the main ExpandedRecordHeader that needs domain constraint validation

## Dependencies
- Functions called/Symbols referenced:
  - expanded_record_get_tupdesc
  - get_short_term_cxt
  - MemoryContextAlloc
  - EOH_init_header
  - ER_MAGIC
- Called from (representative examples):
  - check_domain_for_new_field
  - check_domain_for_new_tuple

## Notes and Other Information
- Function is marked static, indicating internal use within expandedrecord.c only
- Dummy header is marked with ER_FLAG_IS_DUMMY to distinguish it from regular headers
- Uses the short-term memory context to prevent memory leaks during constraint checking
- Copies composite type identification but not domain-specific flags from main header
- Reuses allocated dummy header across multiple constraint checks for efficiency
- Does not transfer domain flags since constraint checking operates on base type values
- System columns remain available through copied fvalue reference from main header