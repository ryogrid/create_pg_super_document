# out_gistxlogPageSplit

## Location
[src/backend/access/rmgrdesc/gistdesc.c:45-51](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/gistdesc.c#L45-L51)

## Overview
A static function that formats and outputs information about GiST page split WAL records, specifically showing how many pages the original page was split into.

## Definition
```c
static void out_gistxlogPageSplit(StringInfo buf, gistxlogPageSplit *xlrec)
```

## Detailed Description
This function handles the formatting and output of GiST index page split operations stored in WAL records. Page splits are fundamental operations in GiST indexes that occur when a page becomes too full and needs to be divided into multiple pages to maintain the index structure and performance.

The function provides a simple but informative output showing how many pages the original page was split into, which is crucial information for understanding the scope and impact of the split operation during debugging or recovery analysis.

## Parameters / Member Variables
- `buf`: StringInfo buffer where the formatted output will be written
- `xlrec`: Pointer to a gistxlogPageSplit structure containing:
  - `origrlink`: BlockNumber of the right link of the page before split
  - `orignsn`: GistNSN (Next Sequence Number) of the page before split
  - `origleaf`: Boolean indicating whether the split page was a leaf page
  - `npage`: Number of pages resulting from the split (including the original page)
  - `markfollowright`: Boolean flag to set F_FOLLOW_RIGHT flags during recovery

## Dependencies
- Functions called/Symbols referenced:
  - [gistxlogPageSplit](../g/gistxlogPageSplit.md) (struct type)
  - [appendStringInfo](../a/appendStringInfo.md) (StringInfo formatting function)
- Called from (representative examples):
  - [gist_desc](../g/gist_desc.md) (when processing XLOG_GIST_PAGE_SPLIT records)

## Notes and Other Information
- Output format: "page_split: splits to N pages"
- The npage count includes the original page, so a split into 2 pages means 1 new page was created
- Page splits are complex operations that can involve multiple backup blocks in the WAL record
- The function focuses on the most essential information (number of resulting pages) rather than detailed split metadata
- Page splits can occur on both leaf and internal pages of the GiST index
- Located in src/backend/access/rmgrdesc/gistdesc.c at lines 45-51