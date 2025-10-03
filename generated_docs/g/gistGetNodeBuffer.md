# gistGetNodeBuffer

## Location
[src/backend/access/gist/gistbuildbuffers.c:113-180](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistbuildbuffers.c#L113-L180)

## Overview
gistGetNodeBuffer retrieves or creates a node buffer for a specific block number during GiST index construction, managing the association between index blocks and their corresponding buffers.

## Definition

```c
GISTNodeBuffer *
gistGetNodeBuffer(GISTBuildBuffers *gfbb, GISTSTATE *giststate,
				  BlockNumber nodeBlocknum, int level)
```
## Detailed Description
This function serves as the primary interface for accessing node buffers during GiST index building. It first searches the hash table for an existing buffer associated with the given block number. If no buffer exists, it creates and initializes a new one.

When creating a new buffer, the function:
- Initializes the buffer structure with default values (empty state, no associated pages)
- Adds the buffer to the appropriate level in the buffersOnLevels array, expanding the array if necessary
- Strategically places new buffers at the beginning of the level list for optimal cache efficiency during the final emptying phase

The function uses a hash table lookup for O(1) average-case buffer retrieval, making it efficient even for large indexes with many blocks.

## Parameters / Member Variables
- : The GiST build buffers structure containing all buffer management data
- : Current GiST state information (used for context but not directly manipulated in this function)  
- : Block number of the index page for which to retrieve/create a buffer
- : Tree level of the node, used for organizing buffers by level

## Dependencies
- Functions called/Symbols referenced:
  - [hash_search](../h/hash_search.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [repalloc](../r/repalloc.md)
  - [lcons](../l/lcons.md)
- Called from (representative examples):
  - [gistProcessItup](gistProcessItup.md)
  - [gistRelocateBuildBuffersOnSplit](gistRelocateBuildBuffersOnSplit.md)

## Notes and Other Information
- Uses HASH_ENTER mode in hash_search to create entries if they don't exist
- Dynamically expands the buffersOnLevels array as higher tree levels are encountered
- New buffers are prepended to level lists for cache efficiency - newly split pages are flushed before older ones
- Memory allocation occurs in the build context to ensure proper lifetime management
- The queuedForEmptying and isTemp flags are initialized to false for new buffers
- InvalidBlockNumber is used to indicate no associated page buffer initially

## Simplified Source

```c
GISTNodeBuffer *
gistGetNodeBuffer(GISTBuildBuffers *gfbb, GISTSTATE *giststate,
                  BlockNumber nodeBlocknum, int level)
{
    GISTNodeBuffer *nodeBuffer;
    bool found;

    // Find or create node buffer in hash table
    nodeBuffer = (GISTNodeBuffer *) hash_search(gfbb->nodeBuffersTab,
                                               &nodeBlocknum,
                                               HASH_ENTER,
                                               &found);

    if (!found)
    {
        // Initialize new buffer
        MemoryContext oldcxt = MemoryContextSwitchTo(gfbb->context);

        nodeBuffer->blocksCount = 0;
        nodeBuffer->pageBlocknum = InvalidBlockNumber;
        nodeBuffer->pageBuffer = NULL;
        nodeBuffer->queuedForEmptying = false;
        nodeBuffer->isTemp = false;
        nodeBuffer->level = level;

        // Expand buffersOnLevels array if needed
        if (level >= gfbb->buffersOnLevelsLen)
        {
            gfbb->buffersOnLevels = (List **) repalloc(gfbb->buffersOnLevels,
                                                      (level + 1) * sizeof(List *));
            // Initialize new levels
            for (int i = gfbb->buffersOnLevelsLen; i <= level; i++)
                gfbb->buffersOnLevels[i] = NIL;
            gfbb->buffersOnLevelsLen = level + 1;
        }

        // Add buffer to beginning of level list for cache efficiency
        gfbb->buffersOnLevels[level] = lcons(nodeBuffer, gfbb->buffersOnLevels[level]);

        MemoryContextSwitchTo(oldcxt);
    }

    return nodeBuffer;
}
```