# CteScanNext

## Location
[src/backend/executor/nodeCtescan.c:31-144](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeCtescan.c#L31-L144)

## Overview
CteScanNext is the workhorse function for ExecCteScan that retrieves the next tuple from a Common Table Expression (CTE) scan, handling both forward and backward scanning directions and managing the underlying tuplestore.

## Definition

```c
static TupleTableSlot *
CteScanNext(CteScanState *node)
```
## Detailed Description
CteScanNext implements the core tuple retrieval logic for CTE scans by managing a shared tuplestore that contains previously fetched CTE results. The function supports bidirectional scanning and handles three main scenarios:

1. **Tuplestore retrieval**: When tuples are available in the tuplestore, it fetches them directly using the node's read pointer
2. **CTE query execution**: When the tuplestore is exhausted in forward direction and the CTE query hasn't reached EOF, it executes the underlying CTE plan to fetch new tuples
3. **Tuple caching**: New tuples from the CTE query are stored in the tuplestore for future access by this and other CTE scan nodes

The function carefully manages scan direction, handling special cases like reversing direction at tuplestore EOF. It ensures tuple stability by copying CTE query results into the node's own slot, preventing issues when other CTE scan nodes advance the query.

## Parameters / Member Variables
- : CteScanState containing the scan state, including the leader node with shared CTE table, read pointer, and scan tuple slot

## Dependencies
- Functions called/Symbols referenced:
  - ScanDirectionIsForward: Check if scan direction is forward
  - [tuplestore_select_read_pointer](../t/tuplestore_select_read_pointer.md): Select the appropriate read pointer for this node
  - [tuplestore_ateof](../t/tuplestore_ateof.md): Check if tuplestore is at end of file
  - [tuplestore_advance](../t/tuplestore_advance.md): Advance tuplestore position
  - [tuplestore_gettupleslot](../t/tuplestore_gettupleslot.md): Retrieve tuple from tuplestore
  - [ExecProcNode](../E/ExecProcNode.md): Execute the underlying CTE plan node
  - TupIsNull: Check if tuple is null
  - [tuplestore_puttupleslot](../t/tuplestore_puttupleslot.md): Store tuple in tuplestore
  - [ExecCopySlot](../E/ExecCopySlot.md): Copy tuple from one slot to another
  - [ExecClearTuple](../E/ExecClearTuple.md): Clear tuple slot
- Called from (representative examples):
  - [ExecCteScan](../E/ExecCteScan.md): The main CTE scan execution function

## Notes and Other Information
- Uses copy=true when calling tuplestore_gettupleslot because the tuplestore is shared with other nodes that might write to it
- Handles backward scanning by doing an extra fetch when reversing direction at tuplestore EOF
- The eof_cte flag prevents redundant calls to plan nodes that are not robust about being called after returning NULL
- Ensures tuple stability by copying CTE query output into the node's own slot to prevent corruption when other nodes advance the query
- Located at src/backend/executor/nodeCtescan.c:31-144

## Simplified Source

```c
static TupleTableSlot *
CteScanNext(CteScanState *node)
{
    EState *estate = node->ss.ps.state;
    bool forward = ScanDirectionIsForward(estate->es_direction);
    Tuplestorestate *tuplestorestate = node->leader->cte_table;
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

    // Select this node's read pointer in the shared tuplestore
    tuplestore_select_read_pointer(tuplestorestate, node->readptr);
    bool eof_tuplestore = tuplestore_ateof(tuplestorestate);

    // Handle backward scanning at EOF: need extra advance to get previous tuple
    if (!forward && eof_tuplestore) {
        if (!node->leader->eof_cte) {
            if (!tuplestore_advance(tuplestorestate, forward))
                return NULL; // tuplestore is empty
        }
        eof_tuplestore = false;
    }

    // Try to fetch tuple from existing tuplestore data
    if (!eof_tuplestore) {
        if (tuplestore_gettupleslot(tuplestorestate, forward, true, slot))
            return slot;
        if (forward)
            eof_tuplestore = true;
    }

    // If tuplestore exhausted in forward direction, try executing CTE query for new data
    if (eof_tuplestore && !node->leader->eof_cte) {
        TupleTableSlot *cteslot = ExecProcNode(node->cteplanstate);

        if (TupIsNull(cteslot)) {
            node->leader->eof_cte = true;
            return NULL; // CTE query finished
        }

        // Reselect read pointer (subplan might have changed it)
        tuplestore_select_read_pointer(tuplestorestate, node->readptr);

        // Store new tuple in tuplestore for future access
        tuplestore_puttupleslot(tuplestorestate, cteslot);

        // Copy tuple to our slot for stability (other nodes might advance CTE)
        return ExecCopySlot(slot, cteslot);
    }

    // No more tuples available
    return ExecClearTuple(slot);
}
```