# bttextnamecmp

## Location
src/backend/utils/adt/varlena.c: 2716 - 2730

## Overview
The  function implements a three-way comparison between a text type and a name type, returning an integer indicating their relative ordering.

## Definition


## Detailed Description
This function performs a collation-aware comparison between a text value and a name (fixed-length string), returning -1 if the text is less than the name, 0 if they are equal, or +1 if the text is greater than the name. It serves as the counterpart to  with reversed argument order and provides the foundation for text-to-name comparison operations. The function uses  to handle locale-specific sorting rules and character comparison.

## Parameters / Member Variables
- : Text type argument (extracted using )
- : Name type argument (extracted using )

## Dependencies
- Functions called/Symbols referenced:
  - : Extract text argument with possible detoasting
  - : Extract name argument
  - : Perform locale-aware string comparison
  - : Get collation for comparison
- Called from (representative examples):
  - : Less-than comparison (src/backend/utils/adt/varlena.c:2764)
  - : Less-than-or-equal comparison (src/backend/utils/adt/varlena.c:2770)
  - : Greater-than comparison (src/backend/utils/adt/varlena.c:2776)
  - : Greater-than-or-equal comparison (src/backend/utils/adt/varlena.c:2782)

## Notes and Other Information
- Located in src/backend/utils/adt/varlena.c:2716-2730
- Returns int32 result: -1 (less than), 0 (equal), or +1 (greater than)
- Counterpart to  with reversed argument order
- Foundation function for all text-name ordering comparisons
- Uses collation-aware comparison through 
- Properly handles variable-length text data with detoasting
- Frees copied text argument to prevent memory leaks