# findeq

## Location
[src/backend/utils/adt/tsquery_rewrite.c:35-205](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_rewrite.c#L35-L205)

## Overview
The `findeq` function performs pattern matching and replacement within TSQuery tree nodes, searching for a specific subtree pattern and replacing matching portions with a substitution subtree.

## Definition
```c
static QTNode *findeq(QTNode *node, QTNode *ex, QTNode *subs, bool *isfind)
```

## Detailed Description
This function is a core component of PostgreSQL's TSQuery rewriting system. It recursively examines a query tree node to find matches with a given example pattern. When a match is found, it replaces the matching portion with a substitution node. The function handles both exact matches (when the entire node matches the example) and subset matches (when a subset of the node's children match the example pattern).

The function implements sophisticated matching logic for different node types:
- For operator nodes (QI_OPR): Performs exact matching when child counts are equal, or subset matching for associative/commutative operators (AND/OR) when the node has more children than the example
- For value nodes (QI_VAL): Performs direct value comparison using CRC checksums

The function sets the QTN_NOCHANGE flag on successfully modified nodes to prevent redundant recursive processing.

## Parameters / Member Variables
- `node`: The current query tree node being examined for matches
- `ex`: The example/pattern node to search for within the current node
- `subs`: The substitution node to replace matched patterns (can be NULL for deletion)
- `isfind`: Output parameter set to true if a replacement was made

## Dependencies
- Functions called/Symbols referenced:
  - [QTNEq](../Q/QTNEq.md) (tree node equality comparison)
  - [QTNFree](../Q/QTNFree.md) (tree node memory deallocation)
  - [QTNCopy](../Q/QTNCopy.md) (tree node deep copy)
  - [QTNodeCompare](../Q/QTNodeCompare.md) (tree node comparison for sorting)
  - [QTNSort](../Q/QTNSort.md) (tree node sorting)
- Called from (representative examples):
  - [dofindsubquery](../d/dofindsubquery.md)

## Notes and Other Information
- The function relies on pre-sorted child nodes for efficient subset matching in associative/commutative operations
- Uses signature-based filtering for performance optimization before detailed comparison
- Handles the QTN_NOCHANGE flag to avoid redundant processing in recursive scenarios
- The subset matching algorithm works only for AND/OR operators due to their commutative/associative properties
- Memory management is carefully handled with proper cleanup of replaced nodes and copying of substitution nodes

## Simplified Source

```c
static QTNode *
findeq(QTNode *node, QTNode *ex, QTNode *subs, bool *isfind)
{
    // Quick signature and type check
    if ((node->sign & ex->sign) != ex->sign ||
        node->valnode->type != ex->valnode->type ||
        node->flags & QTN_NOCHANGE)
        return node;

    if (node->valnode->type == QI_OPR)
    {
        // Must be same operator
        if (node->valnode->qoperator.oper != ex->valnode->qoperator.oper)
            return node;

        if (node->nchild == ex->nchild)
        {
            // Simple case: exact match when same number of children
            if (QTNEq(node, ex))
            {
                QTNFree(node);
                node = subs ? QTNCopy(subs) : NULL;
                if (node) node->flags |= QTN_NOCHANGE;
                *isfind = true;
            }
        }
        else if (node->nchild > ex->nchild && ex->nchild > 0 &&
                 (node->valnode->qoperator.oper == OP_AND ||
                  node->valnode->qoperator.oper == OP_OR))
        {
            // Subset matching for associative operators (AND/OR)
            bool *matched = palloc0(node->nchild * sizeof(bool));
            int nmatched = 0;
            int i = 0, j = 0;

            // Find matching children in sorted order
            while (i < node->nchild && j < ex->nchild)
            {
                int cmp = QTNodeCompare(node->child[i], ex->child[j]);
                if (cmp == 0)
                {
                    matched[i] = true;
                    nmatched++;
                    i++; j++;
                }
                else if (cmp < 0)
                    i++;
                else
                    break;
            }

            // If all ex children matched, replace them with subs
            if (nmatched == ex->nchild)
            {
                // Remove matched children and insert substitution
                j = 0;
                for (i = 0; i < node->nchild; i++)
                {
                    if (matched[i])
                        QTNFree(node->child[i]);
                    else
                        node->child[j++] = node->child[i];
                }

                if (subs)
                {
                    subs = QTNCopy(subs);
                    subs->flags |= QTN_NOCHANGE;
                    node->child[j++] = subs;
                }

                node->nchild = j;
                QTNSort(node);  // Re-sort for consistency
                *isfind = true;
            }

            pfree(matched);
        }
    }
    else  // QI_VAL
    {
        // Value node: check CRC and exact equality
        if (node->valnode->qoperand.valcrc == ex->valnode->qoperand.valcrc &&
            QTNEq(node, ex))
        {
            QTNFree(node);
            node = subs ? QTNCopy(subs) : NULL;
            if (node) node->flags |= QTN_NOCHANGE;
            *isfind = true;
        }
    }

    return node;
}
```