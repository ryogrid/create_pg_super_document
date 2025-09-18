# JsonBaseObjectInfo

## Location
src/backend/utils/adt/jsonpath_exec.c: 84 - 88

## Overview
Represents a "base object" and its "id" for .keyvalue() evaluation in PostgreSQL's JSON path expression execution.

## Definition


## Detailed Description
JsonBaseObjectInfo is a simple structure used in PostgreSQL's JSON path execution engine to track base objects during .keyvalue() method evaluation. This structure maintains a reference to a JSONB container along with an associated identifier, enabling the system to properly handle nested object traversal and key-value pair extraction in JSON path expressions.

## Parameters / Member Variables
- : Pointer to a JsonbContainer that holds the base JSON object data
- uid=1000(ryo) gid=1000(ryo) groups=1000(ryo),4(adm),20(dialout),24(cdrom),25(floppy),27(sudo),29(audio),30(dip),44(video),46(plugdev),117(netdev),998(ollama),999(docker): Integer identifier associated with the base object for tracking purposes

## Dependencies
- Functions called/Symbols referenced:
  - JsonbContainer
- Called from (representative examples):
  - JsonPathExecContext (as a member)
  - executeItemOptUnwrapTarget
  - executeKeyValueMethod
  - setBaseObject

## Notes and Other Information
- This structure is specifically designed for supporting the .keyvalue() JSON path method
- The structure is lightweight, containing only essential information needed for base object tracking
- Used internally within the JSON path execution context to maintain state during complex path evaluations