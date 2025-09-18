# spgFormLeafTuple

## Location
src/backend/access/spgist/spgutils.c: 863 - 951

## Overview
Constructs a complete SP-GiST leaf tuple containing a heap TID reference and attribute data values, with proper memory layout and null value handling.

## Definition


## Detailed Description
This function creates a properly formatted SP-GiST leaf tuple that stores a reference to a heap tuple along with the indexed attribute values. The function implements the same size calculation logic as SpGistGetLeafTupleSize and then constructs the actual tuple structure.

Key aspects of the tuple formation process:

1. **Null bitmap handling**: Uses the same compatibility logic as SpGistGetLeafTupleSize - single-attribute tuples never use null bitmasks for pre-v14 compatibility, while multi-attribute tuples include a null bitmap only when needed.

2. **Memory allocation**: Allocates zero-initialized memory using palloc0() to ensure clean tuple state.

3. **Header initialization**: Sets up the tuple header including size, next offset (initially invalid), and heap pointer.

4. **Data filling**: Uses heap_fill_tuple() to populate the tuple data area following heap tuple conventions, with conditional null bitmap handling.

5. **Minimum size enforcement**: Ensures the tuple meets minimum size requirements for future dead tuple replacement.

## Parameters / Member Variables
- : SpGistState structure containing index configuration and type descriptors
- : ItemPointer referencing the corresponding heap tuple
- : Array of Datum values for each indexed attribute
- : Array of boolean flags indicating which attributes are null

## Dependencies
- Functions called/Symbols referenced:
  - SpGistState (index state structure)
  - SpGistLeafTuple (return type structure)
  - [heap_compute_data_size](../h/heap_compute_data_size.md) (data size calculation)
  - SGLTHDRSZ (header size macro)
  - SGDTSIZE (dead tuple size constant)
  - SGLT_SET_NEXTOFFSET (next offset setter macro)
  - SGLT_SET_HASNULLMASK (null mask flag setter)
  - [heap_fill_tuple](../h/heap_fill_tuple.md) (data population function)
  - spgKeyColumn (key column identifier)
- Called from (representative examples):
  - [doPickSplit](../d/doPickSplit.md) (during node splitting operations)
  - [spgdoinsert](spgdoinsert.md) (during index insertion)

## Notes and Other Information
- The function must stay synchronized with SpGistGetLeafTupleSize for consistent size calculations
- Uses heap tuple data layout conventions, making leaf tuples similar to regular heap tuples in structure
- The compatibility logic ensures backward compatibility with PostgreSQL versions before v14
- Memory is zero-initialized to avoid uninitialized data in padding areas
- The tuple can later be replaced with a dead tuple marker due to minimum size enforcement