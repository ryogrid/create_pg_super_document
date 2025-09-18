# plperl_modify_tuple

## Location
[src/pl/plperl/plperl.c:1762-1851](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L1762-L1851)

## Overview
Constructs a modified tuple to be returned from a Perl trigger function by converting Perl hash data back to PostgreSQL heap tuple format.

## Definition


## Detailed Description
This function takes modifications made by a Perl trigger function (stored in a Perl hash) and applies them to create a new PostgreSQL heap tuple. It extracts the 'new' key from the trigger data hash, validates that it contains a proper hash reference, and then iterates through each key-value pair to construct the modified tuple. The function performs extensive validation including checking for nonexistent columns, system attributes, and generated columns. It serves as a critical bridge for converting Perl-side data modifications back to PostgreSQL's internal tuple representation.

## Parameters / Member Variables
- : Perl hash containing trigger data, including the 'new' hash with modified column values
- : PostgreSQL trigger data structure containing relation information and tuple descriptors
- : Original heap tuple that serves as the base for modifications

## Dependencies
- Functions called/Symbols referenced:
  - [hv_fetch_string](../h/hv_fetch_string.md) (fetch value from Perl hash by string key)
  - SvOK, SvROK, SvTYPE, SvRV (Perl API macros for type checking)
  - hv_iterinit, hv_iternext (Perl hash iteration functions)
  - [hek2cstr](../h/hek2cstr.md) (convert Perl hash key to C string)
  - [SPI_fnumber](../S/SPI_fnumber.md) (get attribute number by name)
  - TupleDescAttr (get attribute descriptor)
  - [plperl_sv_to_datum](plperl_sv_to_datum.md) (convert Perl scalar to PostgreSQL datum)
  - [heap_modify_tuple](../h/heap_modify_tuple.md) (PostgreSQL function to create modified tuple)
  - [palloc0](palloc0.md), pfree (PostgreSQL memory management)
- Called from (representative examples):
  - [plperl_trigger_handler](plperl_trigger_handler.md)

## Notes and Other Information
- Validates that ->{new} exists and is a hash reference before processing
- Prevents modification of system attributes (attribute numbers <= 0)
- Prevents modification of generated columns 
- Allocates arrays for modified values, nulls, and replacement flags
- Uses SPI_ERROR_NOATTRIBUTE to detect invalid column names
- Properly manages memory allocation and deallocation for temporary arrays
- Returns a new HeapTuple that replaces the original in trigger processing