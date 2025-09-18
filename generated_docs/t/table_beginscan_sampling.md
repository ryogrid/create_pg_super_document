# table_beginscan_sampling

## Location
src/include/access/tableam.h: 973 - 995

## Overview
table_beginscan_sampling is a specialized table scanning function designed for TABLESAMPLE operations, providing maximum control over scan behavior including page-mode visibility checking options.

## Definition
```c
static inline TableScanDesc
table_beginscan_sampling(Relation rel, Snapshot snapshot,
                         int nkeys, struct ScanKeyData *key,
                         bool allow_strat, bool allow_sync,
                         bool allow_pagemode)
```

## Detailed Description
table_beginscan_sampling is specifically designed for TABLESAMPLE scan operations, which require fine-grained control over scanning behavior to implement various sampling algorithms. This function extends the control offered by table_beginscan_strat by additionally allowing control over page-mode visibility checking. TABLESAMPLE scans need this level of control because different sampling methods may have specific requirements for how data is accessed and processed.

The function sets the SO_TYPE_SAMPLESCAN flag to indicate the specialized scan type and conditionally enables strategy, synchronization, and page-mode options based on the caller's requirements.

## Parameters / Member Variables
- `rel`: The relation (table) to be scanned for sampling
- `snapshot`: Snapshot for visibility checking of tuples during the scan
- `nkeys`: Number of scan keys for filtering (0 means no filtering)
- `key`: Array of ScanKeyData structures defining the filter conditions
- `allow_strat`: Whether to allow nondefault buffer access strategies
- `allow_sync`: Whether to allow synchronized scanning
- `allow_pagemode`: Whether to allow page-mode visibility checking

## Dependencies
- Functions called/Symbols referenced:
  - SO_TYPE_SAMPLESCAN (sample scan type flag)
  - SO_ALLOW_STRAT (conditionally set based on allow_strat parameter)
  - SO_ALLOW_SYNC (conditionally set based on allow_sync parameter)
  - SO_ALLOW_PAGEMODE (conditionally set based on allow_pagemode parameter)
  - rd_tableam->scan_begin (table access method function)
- Called from (representative examples):
  - [tablesample_init](tablesample_init.md)

## Notes and Other Information
- Designed specifically for TABLESAMPLE operations which implement various statistical sampling algorithms
- Provides the most granular control among all table scan functions, including page-mode visibility checking
- Different sampling methods may require different combinations of these options for optimal performance
- The allow_pagemode parameter is unique to this function and crucial for certain sampling algorithms
- TABLESAMPLE scans are used for statistical analysis and approximate query processing
- The flexibility in controlling scan options allows for implementation of different sampling strategies (SYSTEM, BERNOULLI, etc.)