# gistinserttuple

## Location
src/backend/access/gist/gist.c: 1255 - 1288

## Overview
A convenience wrapper function that inserts or replaces a single tuple in a GiST index page by calling the more general gistinserttuples function.

## Definition


## Detailed Description
gistinserttuple is a simplified interface to the GiST tuple insertion mechanism. It handles the common case of inserting or replacing a single tuple on a GiST index page. The function acts as a thin wrapper around gistinserttuples, converting single-tuple operations into the multi-tuple format expected by the underlying implementation.

When oldoffnum is valid, the function replaces the existing tuple at that offset with the new tuple. When oldoffnum is invalid, the tuple is inserted as a new entry on the page. The function may trigger page splits if the page cannot accommodate the tuple, and returns a boolean indicating whether a split occurred.

The caller must hold an exclusive lock on the target buffer before calling this function. The lock remains held upon return, though the page content may have changed due to splits.

## Parameters / Member Variables
- : GISTInsertState containing insertion context including relation, free space info, and build state
- : GISTInsertStack representing the path from root to the target page being updated
- : GISTSTATE containing cached information about the index's access methods and support functions
- : IndexTuple to be inserted or used as replacement
- : OffsetNumber of existing tuple to replace (InvalidOffsetNumber for new insertion)

## Dependencies
- Functions called/Symbols referenced:
  - gistinserttuples (main implementation)
  - GISTInsertState (state parameter type)
  - GISTInsertStack (stack parameter type)
  - GISTSTATE (giststate parameter type)
- Called from (representative examples):
  - gistdoinsert (main insertion logic)

## Notes and Other Information
- This function is a convenience wrapper that simplifies the common single-tuple insertion case
- All complex logic is delegated to gistinserttuples with appropriate parameters
- The function passes InvalidBuffer for leftchild and rightchild parameters since single tuple insertions don't involve sibling page management
- Return value indicates whether the target page was split during the operation
- Exclusive locking is required for safe tuple insertion and potential page modification