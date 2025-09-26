# SerialPagePrecedesLogically

## Location
src/backend/storage/lmgr/predicate.c: 731 - 746

## Overview
Determines whether one serial page number logically precedes another for truncation purposes in PostgreSQL's serializable isolation implementation.

## Definition

```c
static bool
SerialPagePrecedesLogically(int64 page1, int64 page2)
```
## Detailed Description
This function is analogous to  and is used to determine the logical order of serial pages for SLRU (Simple Least Recently Used) buffer management in the serializable isolation system. It converts page numbers to transaction IDs and uses transaction ID precedence logic to determine ordering.

The function works by:
1. Converting each page number to a representative transaction ID by multiplying by 
2. Adding  to get valid transaction IDs
3. Using  to check if page1's transaction range entirely precedes page2's transaction range
4. Ensuring that page1's highest transaction ID still precedes page2's lowest transaction ID

## Parameters / Member Variables
- : The first serial page number to compare
- : The second serial page number to compare

## Dependencies
- Functions called/Symbols referenced:
  - 
  - 
  - 
- Called from (representative examples):
  - 
  - 
  - 
  - 
  - 

## Notes and Other Information
- This function is crucial for SLRU page management in the serializable isolation subsystem
- The logic ensures that entire transaction ID ranges represented by pages are compared properly
- Used as a callback function pointer in the SLRU control structure ()
- The function handles PostgreSQL's circular transaction ID space correctly through 