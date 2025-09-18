# GistBuildMode

## Location
[src/backend/access/gist/gistbuild.c:79-110](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistbuild.c#L79-L110)

## Overview
An enumeration that defines the strategy used to build a GiST (Generalized Search Tree) index, controlling whether to use buffering, sorting, or regular build modes.

## Definition
```c
typedef enum
{
    GIST_SORTED_BUILD,          /* bottom-up build by sorting */
    GIST_BUFFERING_DISABLED,    /* in regular build mode and aren't going to switch */
    GIST_BUFFERING_AUTO,        /* in regular build mode, but will switch to buffering build mode if the index grows too big */
    GIST_BUFFERING_STATS,       /* gathering statistics of index tuple size before switching to the buffering build mode */
    GIST_BUFFERING_ACTIVE,      /* in buffering build mode */
} GistBuildMode;
```

## Detailed Description
GistBuildMode defines the build strategy for GiST index construction. The mode can dynamically change between the GIST_BUFFERING_* modes during construction, but if the sorted method (GIST_SORTED_BUILD) is chosen, it must be decided up-front and cannot be changed afterwards. The buffering modes allow the system to optimize build performance by switching strategies based on index size and tuple characteristics.

## Parameters / Member Variables
- `GIST_SORTED_BUILD`: Uses a bottom-up build approach by sorting tuples first, requiring good linearization of the sort opclass
- `GIST_BUFFERING_DISABLED`: Operates in regular build mode with no intention to switch to buffering
- `GIST_BUFFERING_AUTO`: Starts in regular build mode but will automatically switch to buffering mode if the index grows too large
- `GIST_BUFFERING_STATS`: Intermediate mode that gathers statistics on index tuple sizes to determine optimal buffering strategy
- `GIST_BUFFERING_ACTIVE`: Currently operating in buffering build mode for performance optimization

## Dependencies
- Functions called/Symbols referenced:
  - Used within GISTBuildState struct
- Called from (representative examples):
  - Various functions in gistbuild.c that manage build state transitions

## Notes and Other Information
The choice of build mode significantly impacts GiST index construction performance. The buffering modes are designed to handle large datasets efficiently by managing memory usage and I/O patterns, while the sorted build mode provides optimal structure for indexes with good sort opclass linearization properties.