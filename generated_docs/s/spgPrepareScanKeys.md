# spgPrepareScanKeys

## Location
[src/backend/access/spgist/spgscan.c:208-303](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgscan.c#L208-L303)

## Overview
Prepares scan keys from caller-given scan keys in the SpGistScanOpaque structure, processing null-related conditions and eliminating null considerations for opclass consistent functions.

## Definition
```c
static void spgPrepareScanKeys(IndexScanDesc scan)
```

## Detailed Description
This static function processes the scan keys provided by the caller and prepares them for use in SP-GiST index scanning. It handles null-related logic by separating IS NULL and IS NOT NULL conditions from regular scan conditions, setting appropriate flags (searchNulls, searchNonNulls) to control which parts of the index need to be scanned.

The function assumes all SPGiST-indexable operators are strict, meaning any null RHS value makes the scan condition unsatisfiable. It processes order-by clauses by removing NULL keys while maintaining offset mappings. For regular scan keys, it filters out null arguments and separates null-specific conditions (IS NULL/IS NOT NULL) from ordinary qualifiers.

## Parameters / Member Variables
- `scan`: IndexScanDesc containing the scan descriptor with keys and order-by clauses to process

## Dependencies
- Functions called/Symbols referenced:
  - SpGistScanOpaque (type)
  - [IndexScanDesc](../I/IndexScanDesc.md) (type)
  - ScanKey (type)
  - SK_ISNULL (flag)
  - SK_SEARCHNULL (flag)
  - SK_SEARCHNOTNULL (flag)
- Called from:
  - [spgrescan](spgrescan.md) (src/backend/access/spgist/spgscan.c:419)

## Notes and Other Information
- This is a static function internal to the spgscan.c module
- Sets searchNulls, searchNonNulls, numberOfKeys, and keyData fields of the SpGistScanOpaque structure
- Handles the logic that IS NULL combined with other conditions is unsatisfiable
- Processes order-by data by removing NULL keys and maintaining offset mappings for non-NULL keys
- Assumes SPGiST operators are strict (null inputs produce null outputs)
- If no qualifiers are provided, enables both null and non-null searches for a whole-index scan

## Simplified Source

```c
static void spgPrepareScanKeys(IndexScanDesc scan) {
    SpGistScanOpaque so = (SpGistScanOpaque) scan->opaque;
    bool qual_ok = true;
    bool haveIsNull = false;
    bool haveNotNull = false;
    int nkeys = 0;

    // Process ORDER BY clauses - copy order-by data and remove NULLs
    so->numberOfOrderBys = scan->numberOfOrderBys;
    so->orderByData = scan->orderByData;

    if (so->numberOfOrderBys > 0) {
        int j = 0;
        // Remove NULL keys but track their original positions
        for (int i = 0; i < scan->numberOfOrderBys; i++) {
            ScanKey skey = &so->orderByData[i];
            if (skey->sk_flags & SK_ISNULL) {
                so->nonNullOrderByOffsets[i] = -1;  // Mark as NULL
            } else {
                if (i != j)
                    so->orderByData[j] = *skey;
                so->nonNullOrderByOffsets[i] = j++;
            }
        }
        so->numberOfNonNullOrderBys = j;
    } else {
        so->numberOfNonNullOrderBys = 0;
    }

    // Handle case with no scan keys - scan entire index
    if (scan->numberOfKeys <= 0) {
        so->searchNulls = true;
        so->searchNonNulls = true;
        so->numberOfKeys = 0;
        return;
    }

    // Process scan keys and separate null-related conditions
    for (int i = 0; i < scan->numberOfKeys; i++) {
        ScanKey skey = &scan->keyData[i];

        if (skey->sk_flags & SK_SEARCHNULL) {
            haveIsNull = true;              // IS NULL condition
        } else if (skey->sk_flags & SK_SEARCHNOTNULL) {
            haveNotNull = true;             // IS NOT NULL condition
        } else if (skey->sk_flags & SK_ISNULL) {
            qual_ok = false;                // NULL argument makes scan unsatisfiable
            break;
        } else {
            so->keyData[nkeys++] = *skey;   // Regular qualifier
            haveNotNull = true;             // Implies NOT NULL requirement
        }
    }

    // IS NULL + other conditions = unsatisfiable
    if (haveIsNull && haveNotNull)
        qual_ok = false;

    // Set final scan parameters
    if (qual_ok) {
        so->searchNulls = haveIsNull;
        so->searchNonNulls = haveNotNull;
        so->numberOfKeys = nkeys;
    } else {
        // Unsatisfiable condition - don't search anything
        so->searchNulls = false;
        so->searchNonNulls = false;
        so->numberOfKeys = 0;
    }
}
```