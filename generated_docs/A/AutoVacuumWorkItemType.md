# AutoVacuumWorkItemType

## Location
[src/include/postmaster/autovacuum.h:26-69](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/postmaster/autovacuum.h#L26-L69)

## Overview
An enumeration that defines the types of work items that can be requested from the autovacuum system by other PostgreSQL processes.

## Definition


## Detailed Description
AutoVacuumWorkItemType is an enumeration that specifies the different categories of work that can be delegated to autovacuum workers from other PostgreSQL processes. Currently, it contains only one value, but the enum structure allows for future expansion of autovacuum work types.

The enum is used in conjunction with the AutoVacuumWorkItem structure to create work requests that are stored in shared memory and processed asynchronously by autovacuum workers. This mechanism allows other processes to offload maintenance tasks to the autovacuum subsystem without blocking their own operations.

The work items are managed through a shared memory array (AutoVacuumShmem->av_workItems) and are protected by the AutovacuumLock, ensuring thread-safe access in a multi-process environment.

## Parameters / Member Variables
- : Requests summarization of a specific BRIN index page range. This work type is used when BRIN indexes with autosummarize enabled detect that a page range needs to be summarized to maintain index effectiveness.

## Dependencies
- Functions called/Symbols referenced:
  - Used in AutoVacuumRequestWork function
  - Used in AutoVacuumWorkItem structure definition
- Called from (representative examples):
  - [brininsert](../b/brininsert.md) (src/backend/access/brin/brin.c:260)
  - do_autovacuum_work (src/backend/postmaster/autovacuum.c:3015)
  - [autovac_report_workitem](../a/autovac_report_workitem.md) (src/backend/postmaster/autovacuum.c:3080)

## Notes and Other Information
- Located in src/include/postmaster/autovacuum.h:23-26
- The enum currently contains only one work type (AVW_BRINSummarizeRange), but is designed to be extensible
- BRIN summarization requests are triggered when BRIN indexes have the autosummarize option enabled and detect unsummarized page ranges during insertion operations
- Work items are processed by autovacuum workers using the brin_summarize_range() function
- The AutoVacuumRequestWork function returns false if the work item array is full, in which case a LOG message is generated
- This mechanism helps maintain BRIN index performance by ensuring that page ranges are summarized in a timely manner without blocking foreground operations