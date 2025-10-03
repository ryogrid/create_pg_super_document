# makeSublist

## Location
[src/backend/access/gin/ginfast.c:145-218](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginfast.c#L145-L218)

## Overview
A static function that splits an array of index tuples across multiple pages and creates a linked list structure of pending-list pages, updating metadata about the resulting sublist.

## Definition

```c
static void
makeSublist(Relation index, IndexTuple *tuples, int32 ntuples,
			GinMetaPageData *res)
```
## Detailed Description
This function is responsible for organizing index tuples into a chain of pending-list pages when the tuples exceed the capacity of a single page. It iterates through the tuple array, calculating space requirements for each tuple (including alignment and item identifier overhead), and creates new pages when the current page would exceed GinListPageSize. The function maintains a linked list structure by setting rightlink pointers between consecutive pages. It updates the provided GinMetaPageData structure with information about the created sublist including head and tail block numbers, tail page free space, and page counts.

## Parameters / Member Variables
- `index`: The GIN index relation being modified
- `tuples`: Array of IndexTuple pointers to be organized into pages
- `ntuples`: Number of tuples in the array
- `res`: Pointer to GinMetaPageData structure to be updated with sublist information

## Dependencies
- Functions called/Symbols referenced:
  - [GinNewBuffer](../G/GinNewBuffer.md)
  - [writeListPage](../w/writeListPage.md)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - IndexTupleSize
  - MAXALIGN
  - GinListPageSize
- Called from (representative examples):
  - [ginHeapTupleFastInsert](../g/ginHeapTupleFastInsert.md)

## Notes and Other Information
- Assumes ntuples > 0 and includes an assertion to verify this
- Calculates tuple size including MAXALIGN alignment and ItemIdData overhead
- Creates multiple pages when tuples don't fit in GinListPageSize
- Sets head block number on first page creation
- Sets tail block number and calculates tail free space on last page
- Always sets nPendingHeapTuples to 1 (represents one heap tuple generating multiple index tuples)
- Uses InvalidBlockNumber as rightlink for the final page to mark end of list
- Part of GIN's fast insertion mechanism for handling large tuple sets

## Simplified Source

```c
// Simplified version of makeSublist
static void makeSublist(Relation index, IndexTuple *tuples, int32 ntuples,
                       GinMetaPageData *res)
{
    Buffer curBuffer = InvalidBuffer;
    Buffer prevBuffer = InvalidBuffer;
    int i, size = 0, tupsize;
    int startTuple = 0;

    Assert(ntuples > 0);

    // Split tuples into pages
    for (i = 0; i < ntuples; i++) {
        if (curBuffer == InvalidBuffer) {
            // Get new buffer
            curBuffer = GinNewBuffer(index);

            if (prevBuffer != InvalidBuffer) {
                // Write previous page with link to current
                res->nPendingPages++;
                writeListPage(index, prevBuffer,
                             tuples + startTuple,
                             i - startTuple,
                             BufferGetBlockNumber(curBuffer));
            } else {
                // First page becomes head
                res->head = BufferGetBlockNumber(curBuffer);
            }

            prevBuffer = curBuffer;
            startTuple = i;
            size = 0;
        }

        tupsize = MAXALIGN(IndexTupleSize(tuples[i])) + sizeof(ItemIdData);

        if (size + tupsize > GinListPageSize) {
            // Won't fit, start new page
            i--;
            curBuffer = InvalidBuffer;
        } else {
            size += tupsize;
        }
    }

    // Write last page
    res->tail = BufferGetBlockNumber(curBuffer);
    res->tailFreeSize = writeListPage(index, curBuffer,
                                     tuples + startTuple,
                                     ntuples - startTuple,
                                     InvalidBlockNumber);
    res->nPendingPages++;
    res->nPendingHeapTuples = 1;
}
```