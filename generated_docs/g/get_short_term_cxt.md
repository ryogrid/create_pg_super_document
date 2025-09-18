# get_short_term_cxt

## Location
[src/backend/utils/adt/expandedrecord.c:1379-1401](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/expandedrecord.c#L1379-L1401)

## Overview
Creates or resets a short-lived memory context for temporary operations within expanded record processing, primarily used for domain checks and detoasting operations.

## Definition


## Detailed Description
This static function manages a dedicated memory context for short-term operations within expanded records. It follows a lazy initialization pattern - creating the context only when first needed, and subsequently resetting it to clear any accumulated memory allocations from previous operations. The context uses small allocation sizes since it's intended for brief, lightweight operations rather than large data structures.

The function prevents memory leaks during domain constraint checking and detoasting operations by providing a dedicated space that can be easily cleaned up after each operation cycle.

## Parameters / Member Variables
- : Pointer to the ExpandedRecordHeader that will own the short-term context

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - ALLOCSET_SMALL_SIZES
  - [MemoryContextReset](../M/MemoryContextReset.md)
- Called from (representative examples):
  - [expanded_record_set_tuple](../e/expanded_record_set_tuple.md)
  - [expanded_record_set_field_internal](../e/expanded_record_set_field_internal.md)
  - [expanded_record_set_fields](../e/expanded_record_set_fields.md)
  - [build_dummy_expanded_header](../b/build_dummy_expanded_header.md)
  - [check_domain_for_new_tuple](../c/check_domain_for_new_tuple.md)

## Notes and Other Information
- Function is marked static, indicating internal use within expandedrecord.c only
- Uses ALLOCSET_SMALL_SIZES to optimize memory allocation for small, temporary objects
- Context is created as a child of the expanded record's main context (erh->hdr.eoh_context)
- Reset pattern avoids repeated allocation/deallocation overhead while preventing memory leaks
- Primarily used for domain constraint evaluation and TOAST value decompression operations