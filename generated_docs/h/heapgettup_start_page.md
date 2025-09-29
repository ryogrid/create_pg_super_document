# heapgettup_start_page

## Location
[src/backend/access/heap/heapam.c:721-751](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L721-L751)

## Overview
heapgettup_start_page is a helper function that prepares page scanning parameters by determining the starting offset and line count for tuple-by-tuple scanning in both forward and backward directions.

## Definition

```c
static Page
heapgettup_start_page(HeapScanDesc scan, ScanDirection dir, int *linesleft,
					  OffsetNumber *lineoff)
```
## Detailed Description
This function initializes the scanning parameters for a heap page that's already loaded in the scan descriptor's current buffer. It calculates the total number of tuples available on the page and sets the appropriate starting offset based on scan direction. For forward scans, it starts from FirstOffsetNumber (the beginning of the page), while for backward scans, it starts from the last offset on the page. The function assumes the buffer is already locked by the caller if necessary and simply extracts the page pointer and computes scanning bounds.

## Parameters / Member Variables
- `scan`: HeapScanDesc containing initialized scan state with valid current buffer
- `dir`: ScanDirection indicating forward or backward scanning
- `linesleft`: Output parameter set to the number of tuples available on the page
- `lineoff`: Output parameter set to the starting offset number for scanning

## Dependencies
- Functions called/Symbols referenced:
  - [BufferIsValid](../B/BufferIsValid.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - ScanDirectionIsForward
  - FirstOffsetNumber
- Called from (representative examples):
  - [heapgettup](heapgettup.md)

## Notes and Other Information
- Assumes scan is already initialized (rs_inited must be true)
- Requires valid current buffer (rs_cbuf must be valid)
- Caller is responsible for buffer locking if needed
- Calculates total line count as MaxOffsetNumber - FirstOffsetNumber + 1
- For backward scans, lineoff is set to the total number of lines (last offset)
- Returns the Page pointer extracted from the current buffer
- Essential helper for tuple-by-tuple scanning as opposed to pagemode scanning
- Used in conjunction with heapgettup for individual tuple iteration

## Simplified Source

```c
static Page heapgettup_start_page(HeapScanDesc scan, ScanDirection dir,
                                 int *linesleft, OffsetNumber *lineoff)
{
    Page page;

    Assert(scan->rs_inited);
    Assert(BufferIsValid(scan->rs_cbuf));

    // Get page from current buffer
    page = BufferGetPage(scan->rs_cbuf);

    // Calculate total tuples on page
    *linesleft = PageGetMaxOffsetNumber(page) - FirstOffsetNumber + 1;

    // Set starting offset based on scan direction
    if (ScanDirectionIsForward(dir))
        *lineoff = FirstOffsetNumber;        // Start from beginning
    else
        *lineoff = (OffsetNumber) (*linesleft);  // Start from end

    return page;
}
```