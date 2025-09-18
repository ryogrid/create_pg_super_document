# plperl_build_tuple_result

## Location
[src/pl/plperl/plperl.c:1075-1125](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L1075-L1125)

## Overview
Constructs a PostgreSQL HeapTuple from a Perl hash reference, mapping hash keys to tuple attributes based on the provided tuple descriptor.

## Definition


## Detailed Description
This function converts a Perl hash into a PostgreSQL tuple by mapping hash keys to column names in the tuple descriptor. The function performs the following steps:

1. **Memory Allocation**: Allocates arrays for Datum values and null flags, initializing all attributes as NULL
2. **Hash Iteration**: Iterates through the Perl hash entries
3. **Column Mapping**: For each hash key, finds the corresponding column in the tuple descriptor using SPI_fnumber
4. **Validation**: Ensures the column exists and is not a system attribute
5. **Type Conversion**: Converts Perl scalar values to PostgreSQL Datums using plperl_sv_to_datum
6. **Tuple Construction**: Creates the final HeapTuple using heap_form_tuple

The function handles missing columns gracefully by leaving them as NULL, but throws errors for nonexistent or system columns.

## Parameters / Member Variables
- : Perl hash reference (HV *) containing key-value pairs to convert to tuple attributes
- : Tuple descriptor (TupleDesc) defining the structure and types of the target tuple

## Dependencies
- Functions called/Symbols referenced:
  - dTHX (Perl threading macro)
  - [palloc0](palloc0.md)/palloc (PostgreSQL memory allocation)
  - hv_iterinit/hv_iternext (Perl hash iteration functions)
  - [hek2cstr](../h/hek2cstr.md) (convert Perl hash key to C string)
  - [SPI_fnumber](../S/SPI_fnumber.md) (find attribute number by name)
  - TupleDescAttr (access tuple descriptor attributes)
  - [plperl_sv_to_datum](plperl_sv_to_datum.md) (convert Perl scalar to PostgreSQL Datum)
  - [heap_form_tuple](../h/heap_form_tuple.md) (create HeapTuple from values array)
  - [pfree](pfree.md) (PostgreSQL memory deallocation)
  - ereport/errcode/errmsg (PostgreSQL error reporting)
- Called from:
  - [plperl_hash_to_datum](plperl_hash_to_datum.md) (for converting hash to composite type)
  - [plperl_return_next_internal](plperl_return_next_internal.md) (for returning hash results from set-returning functions)

## Notes and Other Information
- The function assumes 1-based attribute numbering (attn - 1 for array indexing)
- System attributes (attn <= 0) are explicitly rejected with ERRCODE_FEATURE_NOT_SUPPORTED
- Nonexistent columns trigger ERRCODE_UNDEFINED_COLUMN errors
- The function properly manages memory by freeing temporary allocations
- Hash iteration is reset at the end to ensure consistent state
- All unspecified columns in the hash are set to NULL in the resulting tuple
- This function is essential for PL/Perl functions that return composite types or records