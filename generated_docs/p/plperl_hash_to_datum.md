# plperl_hash_to_datum

## Location
[src/pl/plperl/plperl.c:1126-1137](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L1126-L1137)

## Overview
Converts a Perl hash reference to a PostgreSQL Datum representing a composite type or record.

## Definition

```c
static Datum
plperl_hash_to_datum(SV *src, TupleDesc td)
```
## Detailed Description
This function serves as a high-level wrapper around plperl_build_tuple_result, specifically designed to convert Perl hash references into PostgreSQL Datum values. The function:

1. **Reference Extraction**: Extracts the hash value from the Perl scalar reference using SvRV()
2. **Tuple Construction**: Calls plperl_build_tuple_result to convert the hash to a HeapTuple
3. **Datum Conversion**: Converts the HeapTuple to a Datum using HeapTupleGetDatum()

This function is typically used when PL/Perl functions return hash references that need to be converted to PostgreSQL composite types or records. It provides a clean interface for the common pattern of hash-to-composite-type conversion.

## Parameters / Member Variables
- `*src`: Perl scalar reference (SV *) that should contain a reference to a hash (HV)
- `td`: Tuple descriptor (TupleDesc) defining the structure and types of the target composite type
## Dependencies
- Functions called/Symbols referenced:
  - SvRV (Perl macro to extract reference value)
  - [plperl_build_tuple_result](plperl_build_tuple_result.md) (builds HeapTuple from hash)
  - [HeapTupleGetDatum](../H/HeapTupleGetDatum.md) (converts HeapTuple to Datum)
- Called from:
  - [plperl_sv_to_datum](plperl_sv_to_datum.md) (as part of the general Perl-to-PostgreSQL type conversion system)

## Notes and Other Information
- This function assumes that the input SV is actually a reference to a hash; no type checking is performed
- The function is a thin wrapper that simplifies the common use case of converting hash references to composite type Datums
- Memory management for the created HeapTuple is handled by the PostgreSQL memory context system
- This function is part of the PL/Perl type conversion infrastructure that allows seamless data exchange between Perl and PostgreSQL
- The resulting Datum can be used anywhere PostgreSQL expects a composite type value
- Error handling is delegated to plperl_build_tuple_result, which will report appropriate errors for invalid hash contents

## Simplified Source

```c
static Datum
plperl_hash_to_datum(SV *src, TupleDesc td)
{
    // Extract hash from Perl reference and convert to tuple
    HeapTuple tuple = plperl_build_tuple_result((HV *) SvRV(src), td);

    // Convert tuple to Datum and return
    return HeapTupleGetDatum(tuple);
}
```