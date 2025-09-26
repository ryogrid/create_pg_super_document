# SortSupportData

## Location
src/include/utils/sortsupport.h: 60 - 192

## Overview
SortSupportData is a comprehensive structure that provides the framework for accelerated sorting operations in PostgreSQL, containing context information, sorting parameters, and function pointers for optimized comparison operations.

## Definition
```c
typedef struct SortSupportData
{
    /* Context and initialization fields */
    MemoryContext ssup_cxt;        /* Context containing sort info */
    Oid           ssup_collation;  /* Collation to use, or InvalidOid */
    
    /* Sorting parameters */
    bool          ssup_reverse;    /* descending-order sort? */
    bool          ssup_nulls_first; /* sort nulls first? */
    
    /* Workspace fields */
    AttrNumber    ssup_attno;      /* column number to sort */
    void         *ssup_extra;      /* Workspace for opclass functions */
    
    /* Function pointers for optimization */
    int (*comparator)(Datum x, Datum y, SortSupport ssup);
    
    /* Abbreviated key infrastructure */
    bool          abbreviate;
    Datum         (*abbrev_converter)(Datum original, SortSupport ssup);
    bool          (*abbrev_abort)(int memtupcount, SortSupport ssup);
    int           (*abbrev_full_comparator)(Datum x, Datum y, SortSupport ssup);
} SortSupportData;
```

## Detailed Description
SortSupportData is the core structure of PostgreSQL's accelerated sorting framework. It encapsulates all the information needed for optimized sorting operations, including memory context, collation settings, sorting parameters, and most importantly, function pointers that enable various optimization techniques.

The structure supports two primary optimization mechanisms:
1. **Direct Comparator Functions**: Custom comparison functions that avoid the overhead of SQL-callable functions
2. **Abbreviated Key Infrastructure**: An advanced optimization that creates compact representations of data for faster initial comparisons

The abbreviated key system is particularly sophisticated, allowing opclasses to provide a conversion function that creates a simplified, pass-by-value representation of complex data types. When comparisons using abbreviated keys are inconclusive (return 0), the system falls back to full comparisons using the authoritative comparator.

## Parameters / Member Variables

### Context and Configuration Fields
- `ssup_cxt`: Memory context for allocating sort-related data structures
- `ssup_collation`: OID of the collation to use for sorting, or InvalidOid for default
- `ssup_reverse`: Boolean indicating descending order sorting
- `ssup_nulls_first`: Boolean controlling whether NULL values sort before non-NULL values
- `ssup_attno`: Column number being sorted (workspace for callers)
- `ssup_extra`: Opaque pointer for opclass-specific workspace data

### Core Function Pointers
- `comparator`: Primary comparison function returning <0, 0, or >0 for less than, equal, or greater than
- `abbrev_converter`: Optional function to convert original Datum to abbreviated key format
- `abbrev_abort`: Optional function to determine if abbreviated key strategy should be abandoned
- `abbrev_full_comparator`: Authoritative comparator for full comparison when abbreviated comparison is inconclusive

### Abbreviated Key Control
- `abbreviate`: Boolean hint indicating whether abbreviated keys are applicable for this sort operation

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContext
  - AttrNumber  
  - Datum
  - Oid
- Used by (representative examples):
  - SortSupport (typedef pointer)
  - ApplySortComparator
  - ApplySortAbbrevFullComparator
  - Various btree opclass support functions

## Notes and Other Information
- Defined in src/include/utils/sortsupport.h:60-192
- Initialized by core PostgreSQL code before calling BTSORTSUPPORT_PROC functions
- Function pointers are zeroed before opclass initialization and must be set by BTSORTSUPPORT functions
- The comparator function pointer must always be set; other optimizations are optional  
- Abbreviated key infrastructure is designed to optimize CPU cache performance by reducing memory access patterns
- Opclass authors must carefully consider the cardinality and effectiveness of their abbreviated key schemes
- The framework automatically handles NULL value sorting and reverse order based on the configuration flags
- Core PostgreSQL may dynamically switch between abbreviated and full comparison strategies based on runtime performance analysis