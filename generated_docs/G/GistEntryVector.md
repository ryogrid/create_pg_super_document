# GistEntryVector

## Location
src/include/access/gist.h: 237 - 238

## Overview
A data structure that represents a vector (array) of GISTENTRY structs, commonly used as an argument to user-defined GiST methods such as union and picksplit functions.

## Definition


## Detailed Description
GistEntryVector is a fundamental data structure in PostgreSQL's GiST (Generalized Search Tree) access method implementation. It serves as a container for multiple GISTENTRY structures, allowing GiST operator classes to process collections of index entries efficiently. The structure uses a flexible array member to accommodate variable numbers of entries, making it suitable for dynamic operations during index construction and maintenance.

The vector is primarily used in two key GiST operations:
- **Union operations**: Combining multiple entries into a single representative entry
- **Picksplit operations**: Partitioning entries when a node becomes too full during insertion

## Parameters / Member Variables
- : The number of GISTENTRY elements currently stored in the vector
- : A flexible array member containing the actual GISTENTRY structures

## Dependencies
- Functions called/Symbols referenced:
  - GISTENTRY (struct type for individual entries)
  - FLEXIBLE_ARRAY_MEMBER (macro for variable-length arrays)
  - GEVHDRSZ (macro for calculating header size)

- Called from (representative examples):
  - gist_box_union (geometric box union operations)
  - gist_box_picksplit (geometric box splitting)
  - range_gist_union (range type union operations)
  - range_gist_picksplit (range type splitting)
  - gtsvector_union (text search vector union)
  - inet_gist_union (network address union)
  - genericPickSplit (generic splitting algorithm)
  - gistUserPicksplit (user-defined splitting)
  - gistMakeUnionItVec (utility for creating unions)

## Notes and Other Information
- The GEVHDRSZ macro (defined as ) is used to calculate the size of the structure header, excluding the variable-length array portion
- This structure enables efficient batch processing of index entries, which is crucial for GiST index performance
- The flexible array member design allows for memory-efficient storage of varying numbers of entries without requiring separate memory allocations
- Used extensively across different data types that implement GiST indexing, including geometric types, range types, text search vectors, and network addresses