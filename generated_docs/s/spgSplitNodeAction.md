# spgSplitNodeAction

## Location
[src/backend/access/spgist/spgdoinsert.c:1715-1913](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgdoinsert.c#L1715-L1913)

## Overview
Splits an inner tuple into prefix and postfix tuples according to opclass specifications, replacing the original tuple with the prefix and linking it to the newly created postfix tuple.

## Definition


## Detailed Description
The  function implements inner tuple splitting as requested by the opclass choose function. It constructs a new prefix tuple with the specified number of nodes and prefix information, and a postfix tuple containing all original nodes but with updated prefix data. The prefix tuple replaces the original tuple on the current page, while the postfix tuple may be placed on the same page (if space permits) or moved to a new page following triple parity rules. The function ensures proper downlink establishment from the prefix tuple's specified child node to the postfix tuple location.

## Parameters / Member Variables
- : The SP-GiST index relation being modified
- : SP-GiST state information for tuple formation
- : The existing inner tuple to be split
- : Page descriptor for the page containing the inner tuple
- : Choose function output containing split specifications and parameters

## Dependencies
- Functions called/Symbols referenced:
  - [spgFormNodeTuple](spgFormNodeTuple.md)
  - [spgFormInnerTuple](spgFormInnerTuple.md)  
  - [SpGistGetBuffer](../S/SpGistGetBuffer.md)
  - [PageIndexTupleDelete](../P/PageIndexTupleDelete.md)
  - PageAddItem
  - [SpGistPageAddNewItem](../S/SpGistPageAddNewItem.md)
  - [spgUpdateNodeLink](spgUpdateNodeLink.md)
  - SGITITERATE
- Called from (representative examples):
  - [spgdoinsert](spgdoinsert.md)

## Notes and Other Information
The function validates that the opclass provided reasonable split parameters (valid node counts and child node numbers). It cannot be applied to nulls pages and includes special handling for root page splits where both tuples cannot fit on the same page. The postfix tuple inherits the allTheSame property from the original tuple. WAL logging ensures crash recovery capability, and the function manages page allocation using triple parity rules to maintain proper tree structure. The prefix tuple must not exceed the size of the original tuple to ensure it fits in the replacement location.