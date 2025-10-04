# spgdoinsert

## Location
[src/backend/access/spgist/spgdoinsert.c:1914-2357](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgdoinsert.c#L1914-L2357)

## Overview
Primary insertion function for SP-GiST that coordinates the complete process of inserting a tuple into the index, handling tree navigation, space management, and various insertion scenarios.

## Definition

```c
bool
spgdoinsert(Relation index, SpGistState *state,
			ItemPointer heapPtr, Datum *datums, bool *isnulls)
```
## Detailed Description
The  function orchestrates the entire SP-GiST tuple insertion process. It begins by preparing the leaf tuple data, optionally applying compression, and validating size constraints. The function then navigates the tree starting from the appropriate root (null or regular), following opclass-defined choose function guidance. At each inner node, it may perform match (descend), addNode (expand inner tuple), or splitTuple (restructure inner tuple) operations. When reaching a leaf page, it either directly inserts the tuple, moves the entire leaf chain to a new page, or performs a picksplit operation to redistribute tuples across multiple pages.

## Parameters / Member Variables
- `index`: The SP-GiST index relation to insert into
- `*state`: SP-GiST state containing opclass information and configuration
- `heapPtr`: Item pointer to the heap tuple being indexed
- `*datums`: Array of column values for the index tuple
- `*isnulls`: Array of null flags corresponding to datums
## Dependencies
- Functions called/Symbols referenced:
  - [SpGistGetLeafTupleSize](../S/SpGistGetLeafTupleSize.md)
  - [spgFormLeafTuple](spgFormLeafTuple.md)
  - [addLeafTuple](../a/addLeafTuple.md)
  - [checkSplitConditions](../c/checkSplitConditions.md)
  - [moveLeafs](../m/moveLeafs.md)
  - [doPickSplit](../d/doPickSplit.md)
  - [spgMatchNodeAction](spgMatchNodeAction.md)
  - [spgAddNodeAction](spgAddNodeAction.md)
  - [spgSplitNodeAction](spgSplitNodeAction.md)
  - [spgExtractNodeLabels](spgExtractNodeLabels.md)
- Called from (representative examples):
  - [spgistBuildCallback](spgistBuildCallback.md)
  - [spginsert](spginsert.md)

## Notes and Other Information
Returns true on successful insertion, false if insertion failed due to conflicts (requiring retry by caller). The function includes comprehensive interrupt handling to prevent infinite loops from broken opclasses, with progress tracking for tuple size reduction during prefix stripping. It manages buffer locking carefully to avoid deadlocks during tree descent, using conditional locking and retry mechanisms. The function supports both regular and null value insertion, routing to appropriate tree sections. Size validation prevents oversized tuples from being inserted unless the opclass supports long value handling through successive prefix stripping operations.

## Simplified Source

```c
bool spgdoinsert(Relation index, SpGistState *state,
                 ItemPointer heapPtr, Datum *datums, bool *isnulls) {
    bool result = true;
    bool isnull = isnulls[spgKeyColumn];
    int level = 0;
    Datum leafDatums[INDEX_MAX_KEYS];
    SPPageDesc current, parent;

    // Get choose function for non-null values
    FmgrInfo *procinfo = NULL;
    if (!isnull)
        procinfo = index_getprocinfo(index, 1, SPGIST_CHOOSE_PROC);

    // Prepare leaf datum - apply compression if available
    if (!isnull) {
        if (OidIsValid(index_getprocid(index, 1, SPGIST_COMPRESS_PROC))) {
            FmgrInfo *compressProcinfo = index_getprocinfo(index, 1, SPGIST_COMPRESS_PROC);
            leafDatums[spgKeyColumn] = FunctionCall1Coll(compressProcinfo,
                                                         index->rd_indcollation[spgKeyColumn],
                                                         datums[spgKeyColumn]);
        } else {
            // Handle variable-length values and detoasting
            if (state->attType.attlen == -1)
                leafDatums[spgKeyColumn] = PointerGetDatum(PG_DETOAST_DATUM(datums[spgKeyColumn]));
            else
                leafDatums[spgKeyColumn] = datums[spgKeyColumn];
        }
    } else {
        leafDatums[spgKeyColumn] = (Datum) 0;
    }

    // Prepare INCLUDE column values
    for (int i = spgFirstIncludeColumn; i < state->leafTupDesc->natts; i++) {
        if (!isnulls[i] && TupleDescAttr(state->leafTupDesc, i)->attlen == -1)
            leafDatums[i] = PointerGetDatum(PG_DETOAST_DATUM(datums[i]));
        else
            leafDatums[i] = isnulls[i] ? (Datum) 0 : datums[i];
    }

    // Check tuple size constraints
    int leafSize = SpGistGetLeafTupleSize(state->leafTupDesc, leafDatums, isnulls);
    leafSize += sizeof(ItemIdData);

    if (leafSize > SPGIST_PAGE_CAPACITY && (isnull || !state->config.longValuesOK))
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                       errmsg("index row size %zu exceeds maximum %zu for index \"%s\"",
                              leafSize - sizeof(ItemIdData),
                              SPGIST_PAGE_CAPACITY - sizeof(ItemIdData),
                              RelationGetRelationName(index))));

    // Initialize navigation to appropriate root
    current.blkno = isnull ? SPGIST_NULL_BLKNO : SPGIST_ROOT_BLKNO;
    current.buffer = InvalidBuffer;
    parent.buffer = InvalidBuffer;

    // Main insertion loop
    for (;;) {
        bool isNew = false;

        // Handle interrupts to prevent infinite loops
        if (INTERRUPTS_PENDING_CONDITION()) {
            result = false;
            break;
        }

        // Acquire buffer for current page
        if (current.blkno == InvalidBlockNumber) {
            // Create new leaf page
            current.buffer = SpGistGetBuffer(index,
                                           GBUF_LEAF | (isnull ? GBUF_NULLS : 0),
                                           Min(leafSize, SPGIST_PAGE_CAPACITY),
                                           &isNew);
        } else if (parent.buffer == InvalidBuffer) {
            current.buffer = ReadBuffer(index, current.blkno);
            LockBuffer(current.buffer, BUFFER_LOCK_EXCLUSIVE);
        } else if (current.blkno != parent.blkno) {
            // Conditional locking to avoid deadlocks
            current.buffer = ReadBuffer(index, current.blkno);
            if (!ConditionalLockBuffer(current.buffer)) {
                ReleaseBuffer(current.buffer);
                UnlockReleaseBuffer(parent.buffer);
                return false;  // Retry needed
            }
        } else {
            current.buffer = parent.buffer;
        }

        current.page = BufferGetPage(current.buffer);

        if (SpGistPageIsLeaf(current.page)) {
            // Handle leaf page insertion
            SpGistLeafTuple leafTuple = spgFormLeafTuple(state, heapPtr, leafDatums, isnulls);

            if (leafTuple->size + sizeof(ItemIdData) <= SpGistPageGetFreeSpace(current.page, 1)) {
                // Tuple fits - insert and done
                addLeafTuple(index, state, leafTuple, &current, &parent, isnull, isNew);
                break;
            } else {
                int nToSplit, sizeToSplit;
                if ((sizeToSplit = checkSplitConditions(index, state, &current, &nToSplit)) < SPGIST_PAGE_CAPACITY / 2 &&
                    nToSplit < 64 &&
                    leafTuple->size + sizeof(ItemIdData) + sizeToSplit <= SPGIST_PAGE_CAPACITY) {
                    // Move entire chain to new page
                    moveLeafs(index, state, &current, &parent, leafTuple, isnull);
                    break;
                } else {
                    // Perform picksplit operation
                    if (doPickSplit(index, state, &current, &parent, leafTuple, level, isnull, isNew))
                        break;

                    pfree(leafTuple);
                    goto process_inner_tuple;
                }
            }
        } else {
            // Handle inner page - apply choose function
            SpGistInnerTuple innerTuple;
            spgChooseIn in;
            spgChooseOut out;

        process_inner_tuple:
            innerTuple = (SpGistInnerTuple) PageGetItem(current.page,
                                                       PageGetItemId(current.page, current.offnum));

            // Setup choose function input
            in.datum = datums[spgKeyColumn];
            in.leafDatum = leafDatums[spgKeyColumn];
            in.level = level;
            in.allTheSame = innerTuple->allTheSame;
            in.hasPrefix = (innerTuple->prefixSize > 0);
            in.prefixDatum = SGITDATUM(innerTuple, state);
            in.nNodes = innerTuple->nNodes;
            in.nodeLabels = spgExtractNodeLabels(state, innerTuple);

            memset(&out, 0, sizeof(out));

            // Call choose function or force match for nulls
            if (!isnull) {
                FunctionCall2Coll(procinfo, index->rd_indcollation[0],
                                 PointerGetDatum(&in), PointerGetDatum(&out));
            } else {
                out.resultType = spgMatchNode;
            }

            // Handle allTheSame constraint
            if (innerTuple->allTheSame && out.resultType == spgMatchNode) {
                out.result.matchNode.nodeN = pg_prng_uint64_range(&pg_global_prng_state,
                                                                  0, innerTuple->nNodes - 1);
            }

            // Process choose result
            switch (out.resultType) {
                case spgMatchNode:
                    // Descend to child node
                    spgMatchNodeAction(index, state, innerTuple, &current, &parent,
                                     out.result.matchNode.nodeN);
                    level += out.result.matchNode.levelAdd;

                    // Update leaf datum for next iteration
                    if (!isnull) {
                        leafDatums[spgKeyColumn] = out.result.matchNode.restDatum;
                        leafSize = SpGistGetLeafTupleSize(state->leafTupDesc, leafDatums, isnulls);
                        leafSize += sizeof(ItemIdData);
                    }
                    break;

                case spgAddNode:
                    // Add new node to inner tuple
                    spgAddNodeAction(index, state, innerTuple, &current, &parent,
                                   out.result.addNode.nodeN, out.result.addNode.nodeLabel);
                    goto process_inner_tuple;

                case spgSplitTuple:
                    // Split the inner tuple
                    spgSplitNodeAction(index, state, innerTuple, &current, &out);
                    goto process_inner_tuple;

                default:
                    elog(ERROR, "unrecognized SPGiST choose result: %d", (int) out.resultType);
            }
        }
    }

    // Release buffers
    if (current.buffer != InvalidBuffer) {
        SpGistSetLastUsedPage(index, current.buffer);
        UnlockReleaseBuffer(current.buffer);
    }
    if (parent.buffer != InvalidBuffer && parent.buffer != current.buffer) {
        SpGistSetLastUsedPage(index, parent.buffer);
        UnlockReleaseBuffer(parent.buffer);
    }

    CHECK_FOR_INTERRUPTS();
    return result;
}
```