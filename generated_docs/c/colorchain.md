# colorchain

## Location
[src/backend/regex/regc_color.c:984-1000](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_color.c#L984-L1000)

## Overview
The  function adds an arc to the color chain of its associated color, maintaining a doubly-linked list of arcs for efficient color-based arc management.

## Definition

```c
static void
colorchain(struct colormap *cm,
		   struct arc *a)
```
## Detailed Description
This function implements the core mechanism for maintaining color-based arc chains in the regular expression engine. It adds a given arc to the beginning of the doubly-linked list of arcs associated with the arc's color. The function properly manages both forward () and reverse () pointers to maintain list integrity.

The color chain structure allows the regex engine to efficiently find all arcs of a particular color, which is essential for operations like color promotion, arc relabeling, and color optimization. By maintaining these chains, the engine can quickly iterate through all arcs that share the same color without scanning the entire NFA.

## Parameters / Member Variables
- : Pointer to the colormap structure containing color descriptors
- : Pointer to the arc to be added to the color chain

## Dependencies
- Functions called/Symbols referenced:
  - : Structure representing color descriptor information
  - : Self-reference for maintaining forward chain links
- Called from (representative examples):
  - : During color promotion operations
  - : When creating new arcs in the NFA
  - : For chain manipulation during arc removal
  - : During chain cleanup operations

## Notes and Other Information
- This is a static helper function used internally within the regex color processing module
- Maintains a doubly-linked list structure with both forward and reverse pointers
- The function adds arcs to the beginning of the chain for efficiency
- Critical for color-based optimizations in the regex compilation process
- Includes assertion to ensure valid color indices (>= 0)
- Works in conjunction with  to provide complete chain management functionality

## Simplified Source

```c
static void colorchain(struct colormap *cm, struct arc *a) {
    struct colordesc *cd = &cm->cd[a->co];

    // Add arc to beginning of the color's chain
    if (cd->arcs != NULL)
        cd->arcs->colorchainRev = a;  // Update existing head's reverse pointer

    a->colorchain = cd->arcs;    // Point to current head (or NULL)
    a->colorchainRev = NULL;     // New arc becomes head, no previous arc
    cd->arcs = a;                // Update color descriptor's head pointer
}
```