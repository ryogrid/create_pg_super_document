# vacuumRedirectAndPlaceholder

## Location
[src/backend/access/spgist/spgvacuum.c:493-620](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgvacuum.c#L493-L620)

## Overview
Cleans up redirect and placeholder tuples on SP-GiST pages by converting old redirections to placeholders and removing trailing placeholder tuples that don't affect offset numbering.

## Definition
```c
static void vacuumRedirectAndPlaceholder(Relation index, Relation heaprel, Buffer buffer)
```

## Detailed Description
This function performs cleanup of redirect and placeholder tuples on both leaf and inner SP-GiST pages. It operates in two main phases:

**Phase 1 - Redirect to Placeholder Conversion**:
- Scans backwards through the page looking for REDIRECT tuples
- Converts REDIRECT tuples to PLACEHOLDER if they are old enough (no active transactions can see them)
- Uses global visibility testing to determine if redirects can be safely converted
- Tracks the newest XID among converted redirects for snapshot conflict handling

**Phase 2 - Placeholder Removal**:
- Identifies trailing placeholder tuples that can be safely removed
- Only removes placeholders at the end of the page to avoid changing offset numbers of non-placeholder tuples
- Uses bulk deletion for efficiency since the trailing placeholders are in sequential order

The function maintains SP-GiST page statistics (nRedirection, nPlaceholder) and handles both regular and logical decoding catalog relations. All operations are performed within a critical section with proper WAL logging.

## Parameters / Member Variables
- `index`: The SP-GiST index relation being processed
- `heaprel`: The heap relation associated with the index (used for visibility testing)
- `buffer`: Buffer containing the page to clean up (works on both leaf and inner pages)

## Dependencies
- Functions called/Symbols referenced:
  - [GlobalVisTestFor](../G/GlobalVisTestFor.md), GlobalVisTestIsRemovableXid: Visibility testing functions
  - RelationIsAccessibleInLogicalDecoding: Logical decoding support check
  - [PageIndexMultiDelete](../P/PageIndexMultiDelete.md): Bulk tuple deletion
  - SpGistPageGetOpaque: Page opaque data access
  - TransactionIdIsValid, TransactionIdPrecedes: Transaction ID operations
  - XLog functions: WAL logging (XLogBeginInsert, XLogInsert, etc.)
  - [ItemPointerSetInvalid](../I/ItemPointerSetInvalid.md): Invalidates redirect target pointer
- Called from (representative examples):
  - [spgvacuumpage](../s/spgvacuumpage.md): Called for both leaf and inner pages during regular vacuum
  - [spgprocesspending](../s/spgprocesspending.md): Called when processing pages from pending list

## Notes and Other Information
- This is a static function within the spgvacuum.c file
- Unlike other vacuum functions, this works on both leaf and inner pages
- Uses backward scanning to efficiently identify trailing placeholders
- Maintains offset number stability by only removing trailing placeholders
- Includes support for logical decoding through catalog relation handling
- Uses global visibility state for safe redirect-to-placeholder conversion
- Updates page statistics counters (nRedirection, nPlaceholder) maintained in page opaque data
- Performs all operations within critical sections for crash safety
- WAL logging includes snapshot conflict horizon for standby query handling
- The function is conservative in redirect conversion - only converts when definitely safe
- Part of the SP-GiST vacuum subsystem focused on cleaning up non-live tuples
- Handles both transaction visibility and logical decoding requirements
- The backward scan optimization allows efficient identification of removable trailing placeholders

## Simplified Source

```c
static void
vacuumRedirectAndPlaceholder(Relation index, Relation heaprel, Buffer buffer)
{
    Page page = BufferGetPage(buffer);
    SpGistPageOpaque opaque = SpGistPageGetOpaque(page);
    OffsetNumber max = PageGetMaxOffsetNumber(page);
    OffsetNumber firstPlaceholder = InvalidOffsetNumber;
    bool hasNonPlaceholder = false;
    bool hasUpdate = false;
    OffsetNumber itemToPlaceholder[MaxIndexTuplesPerPage];
    OffsetNumber itemnos[MaxIndexTuplesPerPage];
    spgxlogVacuumRedirect xlrec;
    GlobalVisState *vistest;

    // Initialize WAL record and visibility test
    xlrec.isCatalogRel = RelationIsAccessibleInLogicalDecoding(heaprel);
    xlrec.nToPlaceholder = 0;
    xlrec.snapshotConflictHorizon = InvalidTransactionId;
    vistest = GlobalVisTestFor(heaprel);

    START_CRIT_SECTION();

    // Phase 1: Convert old redirect tuples to placeholders
    for (OffsetNumber i = max; i >= FirstOffsetNumber; i--) {
        SpGistDeadTuple dt = (SpGistDeadTuple) PageGetItem(page, PageGetItemId(page, i));

        // Convert REDIRECT to PLACEHOLDER if transaction is old enough
        if (dt->tupstate == SPGIST_REDIRECT &&
            (!TransactionIdIsValid(dt->xid) ||
             GlobalVisTestIsRemovableXid(vistest, dt->xid))) {

            // Convert to placeholder
            dt->tupstate = SPGIST_PLACEHOLDER;
            opaque->nRedirection--;
            opaque->nPlaceholder++;

            // Track for WAL logging
            if (!TransactionIdIsValid(xlrec.snapshotConflictHorizon) ||
                TransactionIdPrecedes(xlrec.snapshotConflictHorizon, dt->xid))
                xlrec.snapshotConflictHorizon = dt->xid;

            ItemPointerSetInvalid(&dt->pointer);
            itemToPlaceholder[xlrec.nToPlaceholder] = i;
            xlrec.nToPlaceholder++;
            hasUpdate = true;
        }

        // Track placeholder positions for removal
        if (dt->tupstate == SPGIST_PLACEHOLDER) {
            if (!hasNonPlaceholder)
                firstPlaceholder = i;
        } else {
            hasNonPlaceholder = true;
        }
    }

    // Phase 2: Remove trailing placeholder tuples
    if (firstPlaceholder != InvalidOffsetNumber) {
        // Build array of trailing placeholder offsets
        for (OffsetNumber i = firstPlaceholder; i <= max; i++)
            itemnos[i - firstPlaceholder] = i;

        OffsetNumber numToDelete = max - firstPlaceholder + 1;
        opaque->nPlaceholder -= numToDelete;

        // Bulk delete trailing placeholders
        PageIndexMultiDelete(page, itemnos, numToDelete);
        hasUpdate = true;
    }

    xlrec.firstPlaceholder = firstPlaceholder;

    // Mark buffer dirty and log changes
    if (hasUpdate)
        MarkBufferDirty(buffer);

    if (hasUpdate && RelationNeedsWAL(index)) {
        XLogBeginInsert();
        XLogRegisterData((char *) &xlrec, SizeOfSpgxlogVacuumRedirect);
        XLogRegisterData((char *) itemToPlaceholder,
                         sizeof(OffsetNumber) * xlrec.nToPlaceholder);
        XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

        XLogRecPtr recptr = XLogInsert(RM_SPGIST_ID, XLOG_SPGIST_VACUUM_REDIRECT);
        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION();
}
```