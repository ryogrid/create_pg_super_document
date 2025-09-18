# FreePageManagerDumpSpans

## Location
src/backend/utils/mmgr/freepage.c: 1296 - 1318

## Overview
A debugging function that generates a formatted dump of linked free page spans, showing page numbers and span sizes for diagnostic purposes.

## Definition
```c
static void FreePageManagerDumpSpans(FreePageManager *fpm, FreePageSpanLeader *span,
                                     Size expected_pages, StringInfo buf)
```

## Detailed Description
This function traverses a linked list of free page spans starting from the given span leader, generating a textual representation of each span's location and size. It formats the output to show page numbers, and when a span's actual page count differs from the expected size, it displays both the page number and actual page count in parentheses. The function follows the linked list using relative pointers until reaching the end of the chain.

## Parameters / Member Variables
- `fpm`: Pointer to the FreePageManager instance containing the spans
- `span`: Starting FreePageSpanLeader in the linked list to dump
- `expected_pages`: Expected number of pages per span for comparison
- `buf`: StringInfo buffer to append the formatted span information

## Dependencies
- Functions called/Symbols referenced:
  - fpm_segment_base
  - fpm_pointer_to_page
  - relptr_access  
  - appendStringInfo
  - appendStringInfoChar
- Called from (representative examples):
  - [FreePageManagerDump](FreePageManagerDump.md) (multiple calls)

## Notes and Other Information
- This is a static function used only for debugging purposes
- Traverses linked lists of free spans using relative pointers
- Highlights discrepancies between expected and actual span sizes
- Outputs page numbers in a compact format for easy analysis
- Terminates output with a newline character