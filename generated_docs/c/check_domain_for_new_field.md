# check_domain_for_new_field

## Location
src/backend/utils/adt/expandedrecord.c: 1494 - 1575

## Overview
Validates domain constraints for a single field assignment operation by creating a temporary record with the proposed new value and running domain checks against it.

## Definition


## Detailed Description
This function performs preemptive domain constraint validation before actually modifying a field in an expanded record. It constructs a dummy expanded record header containing the current record state plus the proposed new field value, then runs domain_check() against this temporary record. This approach ensures that constraint violations are detected before any permanent changes are made to the actual record.

The function handles both empty and populated records appropriately - copying existing field values for populated records, or initializing all fields as null for empty records. It uses the short-term memory context for constraint evaluation to prevent memory leaks from expression evaluation cruft.

## Parameters / Member Variables
- : Pointer to the main ExpandedRecordHeader being modified
- : 1-based field number to be assigned (must be > 0 and <= nfields)
- : The proposed new Datum value for the field
- : Boolean indicating whether the new value is NULL

## Dependencies
- Functions called/Symbols referenced:
  - [build_dummy_expanded_header](../b/build_dummy_expanded_header.md)
  - ExpandedRecordIsEmpty
  - [deconstruct_expanded_record](../d/deconstruct_expanded_record.md)
  - VARATT_IS_EXTERNAL
  - [domain_check](../d/domain_check.md)
  - ExpandedRecordGetRODatum
  - [MemoryContextReset](../M/MemoryContextReset.md)
- Called from (representative examples):
  - [expanded_record_set_field_internal](../e/expanded_record_set_field_internal.md)

## Notes and Other Information
- Function is marked static and pg_noinline, indicating internal use with call-site optimization disabled
- Validates field numbers and throws ERROR for invalid field references (system columns or out-of-range)
- Properly handles external TOAST values by setting ER_FLAG_HAVE_EXTERNAL when needed
- Uses the main header's domain cache space for efficient repeated constraint checking
- Immediately cleans up the short-term context after constraint validation
- Function name suggests it's specifically for single field operations (vs. bulk operations)