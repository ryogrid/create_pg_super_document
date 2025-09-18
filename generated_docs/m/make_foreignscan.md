# make_foreignscan

## Location
src/backend/optimizer/plan/createplan.c: 5823 - 5862

## Overview
Creates and initializes a ForeignScan plan node for accessing data from foreign data sources through Foreign Data Wrappers (FDWs) in PostgreSQL.

## Definition
```c
ForeignScan *
make_foreignscan(List *qptlist,
                List *qpqual,
                Index scanrelid,
                List *fdw_exprs,
                List *fdw_private,
                List *fdw_scan_tlist,
                List *fdw_recheck_quals,
                Plan *outer_plan)
```

## Detailed Description
This function constructs a ForeignScan plan node, which is used to scan foreign tables through Foreign Data Wrappers. FDWs allow PostgreSQL to access external data sources as if they were regular tables. The function initializes various FDW-specific fields that control how the foreign data source is accessed, what expressions are pushed down to the foreign server, and what data needs to be rechecked locally. Unlike basic scan nodes, ForeignScan can have an outer plan for join pushdown scenarios.

## Parameters / Member Variables
- `qptlist`: Target list specifying which columns/expressions to return from the foreign scan
- `qpqual`: List of qualification conditions to apply (may be rechecked locally)
- `scanrelid`: Index identifying the foreign relation being scanned in the query's range table
- `fdw_exprs`: List of expressions that will be sent to the foreign server for evaluation
- `fdw_private`: FDW-specific private data used by the foreign data wrapper
- `fdw_scan_tlist`: Target list for columns actually fetched from the foreign server
- `fdw_recheck_quals`: Qualification conditions that must be rechecked locally after fetching
- `outer_plan`: Optional outer plan node for join pushdown scenarios

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to allocate ForeignScan node)
  - ForeignScan (struct type)
  - CMD_SELECT (operation type constant)
  - InvalidOid (invalid object identifier constant)
- Called from (representative examples):
  - create_foreignscan_plan (inferred from cost comment)

## Notes and Other Information
- Unlike the previous scan functions, this function is not static and can be called from other files
- The function sets default values for operation (CMD_SELECT) and resultRelation (0), which may be overridden by FDW callbacks
- Several fields (checkAsUser, fs_server, fs_relids, fs_base_relids, fsSystemCol) are left to be filled by create_foreignscan_plan
- Cost calculation is deferred to the calling function create_foreignscan_plan
- The outer_plan parameter allows ForeignScan to participate in join pushdown optimizations where joins are executed on the foreign server