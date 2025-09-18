# SpGistGetInnerTypeSize

## Location
src/backend/access/spgist/spgutils.c: 771 - 788

## Overview
Calculates the storage space required for a non-null datum in an SP-GiST inner tuple (used for prefix or node label storage), with the result aligned to MAXALIGN boundary.

## Definition


## Detailed Description
This function determines the appropriate storage size for a datum that will be stored in an SP-GiST inner tuple, which can be either a prefix value or a node label. The function follows the PostgreSQL convention where pass-by-value types are stored in their Datum representation directly. The calculated size is automatically rounded up to a MAXALIGN boundary to ensure proper memory alignment for the target platform.

The size calculation depends on the attribute type descriptor:
- For pass-by-value types: Uses sizeof(Datum) 
- For fixed-length pass-by-reference types: Uses the fixed length from att->attlen
- For variable-length types: Uses the actual size from VARSIZE_ANY() macro

## Parameters / Member Variables
- : Pointer to SpGistTypeDesc structure containing type information (byval flag, length, etc.)
- : The actual datum value for which storage size is being calculated

## Dependencies
- Functions called/Symbols referenced:
  - SpGistTypeDesc (type descriptor structure)
  - VARSIZE_ANY (macro for getting variable-length type size)
  - MAXALIGN (macro for memory alignment)
- Called from (representative examples):
  - spgFormNodeTuple
  - spgFormInnerTuple

## Notes and Other Information
- The function assumes the datum is non-null (as stated in the comment)
- Memory alignment is critical for performance and correctness on different architectures
- This function is part of the SP-GiST (Space-Partitioned Generalized Search Tree) access method implementation
- The size calculation strategy varies based on whether the type is pass-by-value or pass-by-reference