# test_itemptr_pair

## Location
[src/test/modules/test_ginpostinglist/test_ginpostinglist.c:41-87](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_ginpostinglist/test_ginpostinglist.c#L41-L87)

## Overview
A static test function that validates the GIN posting list encoding and decoding functionality by encoding a pair of TIDs and verifying the round-trip conversion.

## Definition

```c
static void
test_itemptr_pair(BlockNumber blk, OffsetNumber off, int maxsize)
```
## Detailed Description
This function tests the GIN posting list compression and decompression by creating a pair of ItemPointers (TIDs), encoding them into a GinPostingList using ginCompressPostingList(), and then decoding them back using ginPostingListDecode(). The function specifically tests with a fixed first TID of (0, 1) and a second TID formed from the provided block and offset parameters.

The function is designed to test the varbyte encoding used for delta compression between TIDs in GIN posting lists. Since the first TID is stored as-is and only subsequent TIDs use delta encoding, testing a pair ensures the varbyte encoding logic is exercised.

The function performs comprehensive validation including:
- Verifying the encoded size doesn't exceed the specified maximum
- Ensuring the number of encoded and decoded items match
- Comparing each decoded TID against the original input
- Reporting detailed notices about the test parameters and results

## Parameters / Member Variables
- `blk`: Block number for the second TID to be tested
- `off`: Offset number for the second TID to be tested
- `maxsize`: Maximum allowed size in bytes for the compressed GinPostingList (used for overflow testing)
## Dependencies
- Functions called/Symbols referenced:
  - elog (for NOTICE and ERROR reporting)
  - [ItemPointerSet](../I/ItemPointerSet.md) (to initialize TID values)
  - [ginCompressPostingList](../g/ginCompressPostingList.md) (to encode the TID pair)
  - SizeOfGinPostingList (to check encoded size)
  - [ginPostingListDecode](../g/ginPostingListDecode.md) (to decode back to TIDs)
  - [ItemPointerEquals](../I/ItemPointerEquals.md) (to compare original and decoded TIDs)
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)/ItemPointerGetOffsetNumber (for error reporting)
- Called from:
  - [test_ginpostinglist](test_ginpostinglist.md) (multiple times with different parameters)

## Notes and Other Information
- This is a static function within the test_ginpostinglist test module
- The function always uses (0, 1) as the first TID to ensure consistent testing of delta encoding
- Any encoding/decoding mismatches or size overflows are reported as ERRORs
- The function provides detailed NOTICE messages to track test progress and results
- Located in src/test/modules/test_ginpostinglist/test_ginpostinglist.c:41-87