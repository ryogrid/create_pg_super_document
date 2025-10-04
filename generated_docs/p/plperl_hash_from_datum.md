# plperl_hash_from_datum

## Location
[src/pl/plperl/plperl.c:2998-3025](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L2998-L3025)

## Overview
Converts a PostgreSQL composite/row datum into a Perl hash reference by extracting tuple information and delegating to the tuple-to-hash conversion function.

## Definition

```c
structure */
	tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
```
## Detailed Description
This function serves as a conversion utility that transforms a PostgreSQL composite datum (representing a row or composite type) into a Perl hash reference. It extracts the tuple header information from the datum, determines the row type and type modifier, looks up the corresponding tuple descriptor, constructs a temporary HeapTuple structure, and then delegates the actual hash creation to plperl_hash_from_tuple. The function handles the proper acquisition and release of the tuple descriptor to ensure resource management.

## Parameters / Member Variables
- : PostgreSQL Datum containing a composite/row value to be converted

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeader: Type for tuple header structures
  - [HeapTupleData](../H/HeapTupleData.md): Type for heap tuple control structures
  - DatumGetHeapTupleHeader: Extracts tuple header from datum
  - HeapTupleHeaderGetTypeId: Gets the type OID from tuple header
  - HeapTupleHeaderGetTypMod: Gets type modifier from tuple header
  - [lookup_rowtype_tupdesc](../l/lookup_rowtype_tupdesc.md): Retrieves tuple descriptor for the row type
  - HeapTupleHeaderGetDatumLength: Gets the length of the tuple data
  - [plperl_hash_from_tuple](plperl_hash_from_tuple.md): Converts tuple to Perl hash
  - ReleaseTupleDesc: Releases the tuple descriptor
- Called from:
  - [make_array_ref](../m/make_array_ref.md): Used in array element processing
  - [plperl_call_perl_func](plperl_call_perl_func.md): Used in function argument conversion
  - [plperl_hash_from_tuple](plperl_hash_from_tuple.md): Used recursively for nested composite types

## Notes and Other Information
- Returns a Perl SV* (scalar value) representing a hash reference
- Handles composite/row types by converting them to Perl hash structures
- Uses temporary HeapTuple structure to interface with existing tuple processing functions
- Properly manages tuple descriptor lifecycle with lookup and release
- Part of the data conversion layer between PostgreSQL and Perl
- Located at src/pl/plperl/plperl.c:2998-3025

## Simplified Source

```c
static SV *
plperl_hash_from_datum(Datum attr)
{
    HeapTupleHeader td;
    Oid tupType;
    int32 tupTypmod;
    TupleDesc tupdesc;
    HeapTupleData tmptup;
    SV *sv;

    // Extract tuple header from datum
    td = DatumGetHeapTupleHeader(attr);

    // Get row type information
    tupType = HeapTupleHeaderGetTypeId(td);
    tupTypmod = HeapTupleHeaderGetTypMod(td);
    tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

    // Create temporary tuple structure
    tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
    tmptup.t_data = td;

    // Convert tuple to Perl hash and clean up
    sv = plperl_hash_from_tuple(&tmptup, tupdesc, true);
    ReleaseTupleDesc(tupdesc);

    return sv;
}
```