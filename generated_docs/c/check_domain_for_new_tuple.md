# check_domain_for_new_tuple

## Location
src/backend/utils/adt/expandedrecord.c: 1576 - 1633

## Overview
Validates domain constraints for a complete tuple replacement operation by creating a temporary record with the new tuple and running domain checks against it.

## Definition


## Detailed Description
This function performs preemptive domain constraint validation before replacing an entire expanded record with a new HeapTuple. It handles two distinct cases: NULL tuple assignment (empty record) and actual tuple assignment. For NULL tuples, it directly validates whether NULL values are acceptable for the domain. For actual tuples, it constructs a dummy expanded record header containing the new tuple and runs domain_check() against it.

Unlike single field validation, this function works with complete tuples and doesn't need to deconstruct fields immediately - it sets up the flattened tuple representation and lets the domain checking mechanism handle field access as needed. The function uses the short-term memory context to prevent memory leaks during constraint evaluation.

## Parameters / Member Variables
- : Pointer to the main ExpandedRecordHeader being modified
- : The new HeapTuple to assign, or NULL to set record as empty

## Dependencies
- Functions called/Symbols referenced:
  - [get_short_term_cxt](../g/get_short_term_cxt.md)
  - [domain_check](../d/domain_check.md)
  - [MemoryContextReset](../M/MemoryContextReset.md)
  - [build_dummy_expanded_header](../b/build_dummy_expanded_header.md)
  - HeapTupleHasExternal
  - ExpandedRecordGetRODatum
- Called from (representative examples):
  - [expanded_record_set_tuple](../e/expanded_record_set_tuple.md)

## Notes and Other Information
- Function is marked static and pg_noinline, indicating internal use with call-site optimization disabled
- Handles NULL tuple assignment as a special case by checking domain constraints on NULL values directly
- For non-NULL tuples, sets up flattened representation without immediate field deconstruction
- Properly detects and flags external TOAST values using HeapTupleHasExternal()
- Uses the main header's domain cache space for efficient repeated constraint checking
- Immediately cleans up the short-term context after constraint validation
- Designed for bulk tuple replacement operations rather than individual field modifications