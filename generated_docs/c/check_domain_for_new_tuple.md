# check_domain_for_new_tuple

## Location
[src/backend/utils/adt/expandedrecord.c:1576-1633](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/expandedrecord.c#L1576-L1633)

## Overview
Validates domain constraints for a complete tuple replacement operation by creating a temporary record with the new tuple and running domain checks against it.

## Definition

```c
struct dummy header to contain replacement tuple */
	build_dummy_expanded_header(erh);
```
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
  - [ExpandedRecordGetRODatum](../E/ExpandedRecordGetRODatum.md)
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

## Simplified Source

```c
static void check_domain_for_new_tuple(ExpandedRecordHeader *erh, HeapTuple tuple)
{
    // Handle NULL tuple (empty record) case
    if (tuple == NULL) {
        MemoryContext oldcxt = MemoryContextSwitchTo(get_short_term_cxt(erh));
        domain_check((Datum) 0, true, erh->er_decltypeid,
                    &erh->er_domaininfo, erh->hdr.eoh_context);
        MemoryContextSwitchTo(oldcxt);
        MemoryContextReset(erh->er_short_term_cxt);
        return;
    }

    // Create dummy header for non-NULL tuple
    build_dummy_expanded_header(erh);
    ExpandedRecordHeader *dummy_erh = erh->er_dummy_header;

    // Set up flattened tuple representation
    dummy_erh->fvalue = tuple;
    dummy_erh->fstartptr = (char *) tuple->t_data;
    dummy_erh->fendptr = ((char *) tuple->t_data) + tuple->t_len;
    dummy_erh->flags |= ER_FLAG_FVALUE_VALID;

    // Track external values
    if (HeapTupleHasExternal(tuple))
        dummy_erh->flags |= ER_FLAG_HAVE_EXTERNAL;

    // Run domain constraint check
    MemoryContext oldcxt = MemoryContextSwitchTo(erh->er_short_term_cxt);
    domain_check(ExpandedRecordGetRODatum(dummy_erh), false,
                erh->er_decltypeid, &erh->er_domaininfo,
                erh->hdr.eoh_context);
    MemoryContextSwitchTo(oldcxt);

    // Clean up
    MemoryContextReset(erh->er_short_term_cxt);
}
```