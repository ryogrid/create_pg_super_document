# BufferManagerRelation

## Location
src/include/storage/bufmgr.h: 100 - 105

## Overview
BufferManagerRelation is a structure that provides a unified way to identify database relations for buffer management operations, supporting both normal operation and recovery scenarios.

## Definition


## Detailed Description
BufferManagerRelation serves as a flexible container for relation identification in PostgreSQL's buffer management system. This structure allows buffer management functions to work with relations in two different modes: using a full Relation object during normal operation, or using just the storage manager (SMgrRelationData) and persistence information during recovery when full relation metadata may not be available.

The structure enables the same buffer management functions to be used in both contexts through the BMR_REL() and BMR_SMGR() convenience macros, providing a clean abstraction that simplifies the buffer management API while supporting the different operational requirements of normal database operation versus recovery scenarios.

## Parameters / Member Variables
- : A Relation pointer used during normal database operation when full relation metadata is available
- : A pointer to SMgrRelationData structure containing storage manager information, used primarily during recovery or when only basic storage information is needed
- : A character indicating the relation's persistence level (permanent, temporary, unlogged), required when working with storage manager directly

## Dependencies
- Functions called/Symbols referenced:
  - SMgrRelationData (structure)
  - Relation (type)
- Called from (representative examples):
  - ExtendBufferedRel (in bufmgr.c:845)
  - ExtendBufferedRelBy (in bufmgr.c:877)
  - ExtendBufferedRelTo (in bufmgr.c:909)
  - ExtendBufferedRelCommon (in bufmgr.c:2135)
  - ExtendBufferedRelShared (in bufmgr.c:2179)
  - ExtendBufferedRelLocal (in localbuf.c:313)

## Notes and Other Information
- Used with convenience macros BMR_REL() and BMR_SMGR() for easy construction
- BMR_REL(p_rel) creates a BufferManagerRelation from a Relation pointer
- BMR_SMGR(p_smgr, p_relpersistence) creates one from storage manager and persistence info
- Essential for buffer extension operations that need to work during both normal operation and recovery
- Allows the same buffer management functions to handle different relation identification scenarios
- The relpersistence field is critical for proper buffer management behavior with different relation types