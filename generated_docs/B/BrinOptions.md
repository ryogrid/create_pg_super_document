# BrinOptions

## Location
[src/include/access/brin.h:21-26](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/brin.h#L21-L26)

## Overview
BrinOptions is a PostgreSQL data structure that stores the reloptions (relation options) configuration for BRIN (Block Range Index) indexes, controlling index behavior such as pages per range and auto-summarization.

## Definition


## Detailed Description
BrinOptions is a varlena structure that encapsulates configuration options for BRIN indexes. BRIN indexes are designed to efficiently index very large tables by storing summary information about ranges of table blocks. This structure allows users to customize two key aspects of BRIN index behavior:

1. **Pages per range**: Controls how many heap pages are summarized in each BRIN index tuple
2. **Auto-summarization**: Controls whether the index automatically creates summaries for new page ranges

The structure follows PostgreSQL's varlena convention, making it suitable for storage as relation options. It's processed by the brinoptions() function during index creation and option changes.

## Parameters / Member Variables
- : Standard varlena header for variable-length data structures (managed by PostgreSQL's varlena infrastructure)
- : Number of heap pages that each BRIN index tuple summarizes (BlockNumber type)
- : Boolean flag indicating whether the index should automatically create summaries for new page ranges

## Dependencies
- Functions called/Symbols referenced:
  - BlockNumber (type dependency)
  - int32 (type dependency)
  - [bool](../b/bool.md) (type dependency)
- Called from (representative examples):
  - [brinoptions](../b/brinoptions.md)() function in src/backend/access/brin/brin.c:1341
  - BrinGetPagesPerRange() macro in src/include/access/brin.h:44
  - BrinGetAutoSummarize() macro in src/include/access/brin.h:50

## Notes and Other Information
- The structure is defined in src/include/access/brin.h:21-26
- Used exclusively with BRIN indexes (Block Range Indexes)
- The pagesPerRange setting affects index size vs. accuracy trade-offs: fewer pages per range means larger indexes but more precise summaries
- Auto-summarization helps maintain index effectiveness as tables grow, but adds overhead during insertions
- The vl_len_ member should never be accessed directly; it's managed by PostgreSQL's varlena system
- Default values are provided when options are not explicitly set (BRIN_DEFAULT_PAGES_PER_RANGE for pagesPerRange, false for autosummarize)