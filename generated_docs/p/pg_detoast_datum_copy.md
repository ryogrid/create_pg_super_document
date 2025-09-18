# pg_detoast_datum_copy

## Location
[src/backend/utils/fmgr/fmgr.c:1841-1856](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L1841-L1856)

## Overview
This function creates a modifiable copy of a varlena datum, detoasting it if necessary to ensure the result is always a pfree'able copy that can be safely modified.

## Definition


## Detailed Description
pg_detoast_datum_copy is a utility function that ensures the caller receives a modifiable copy of a varlena datum. If the input datum is in extended form (compressed, externally stored, etc.), it calls detoast_attr to fully detoast it. If the datum is already in normal form, it creates a palloc'd copy to ensure the result is always modifiable and independently manageable.

This function is particularly useful when a function needs to modify the contents of a varlena value, as it guarantees that the returned pointer points to a separate copy that can be safely modified without affecting the original data. The function is commonly used through the PG_DETOAST_DATUM_COPY macro in PostgreSQL function implementations.

## Parameters / Member Variables
- : A pointer to the varlena structure that may be in extended (toasted) form or normal form

## Dependencies
- Functions called/Symbols referenced:
  - VARATT_IS_EXTENDED (macro to check if datum is in extended form)
  - [detoast_attr](../d/detoast_attr.md) (function to detoast extended datums)
  - VARSIZE (macro to get the size of a varlena)
  - [palloc](palloc.md) (memory allocation function)
  - memcpy (memory copy function)
- Called from (representative examples):
  - PG_DETOAST_DATUM_COPY (macro)
  - Various PostgreSQL functions that need modifiable copies of varlena data

## Notes and Other Information
This function is part of PostgreSQL's TOAST (The Oversized-Attribute Storage Technique) system, which handles large variable-length data. The function guarantees that the returned varlena is always a separate, modifiable copy, making it safe for functions that need to modify the data contents. Unlike pg_detoast_datum which may return the original pointer if no detoasting is needed, this function always returns a new copy, providing predictable memory management semantics.