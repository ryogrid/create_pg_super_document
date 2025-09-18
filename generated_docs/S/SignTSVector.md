# SignTSVector

## Location
src/backend/utils/adt/tsgistidx.c: 68 - 69

## Overview
A structure that represents the type of GiST index key used for tsvector indexing, supporting different key formats including array keys, signature keys, and all-true bitmaps.

## Definition


## Detailed Description
`SignTSVector` is a flexible data structure used as the fundamental building block for GiST index keys in PostgreSQL's full-text search system. It can represent three different types of index keys based on the flag value: array keys (ARRKEY), signature keys (SIGNKEY), and all-true keys (ALLISTRUE). The structure uses a flexible array member to store variable-length data depending on the key type.

Array keys contain arrays of integers, signature keys contain bit signatures for filtering, and all-true keys represent a special case where all bits are considered set. This polymorphic design allows the same structure to efficiently handle different phases of index operations while minimizing memory usage.

## Parameters / Member Variables
- `vl_len_`: Standard PostgreSQL varlena header containing the total size of the structure. This should not be manipulated directly by user code.
- `flag`: Bit flags indicating the type and properties of the key. Can contain combinations of ARRKEY (0x01), SIGNKEY (0x02), and ALLISTRUE (0x04).
- `data`: Flexible array member that stores the actual key data. Content varies based on the flag value - integer arrays for ARRKEY, bit signatures for SIGNKEY, or empty for ALLISTRUE.

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER macro for variable-length data
  - Standard PostgreSQL varlena format
- Called from (representative examples):
  - `GETENTRY`: Macro to extract SignTSVector from GiST entry vector
  - `ISARRKEY`, `ISSIGNKEY`, `ISALLTRUE`: Macros to test flag values
  - `gtsvector_compress`: Compression function for GiST entries
  - `gtsvector_union`: Union operation for combining index keys
  - `gtsvector_consistent`: Consistency check function
  - `gtsvector_penalty`: Penalty calculation for index insertion
  - `gtsvector_picksplit`: Node splitting algorithm

## Notes and Other Information
- The structure supports three distinct key types controlled by flag bits:
  - ARRKEY (0x01): Array of integer values
  - SIGNKEY (0x02): Bit signature for filtering
  - ALLISTRUE (0x04): Special case indicating all bits are set
- Size calculation is handled by the `CALCGTSIZE` macro which accounts for different data layouts
- The `GTHDRSIZE` constant defines the header size (varlena header + flag)
- This structure is central to PostgreSQL's GiST-based full-text search indexing and is used throughout the tsvector indexing pipeline
- The flexible design allows for efficient memory usage and optimal performance across different indexing scenarios