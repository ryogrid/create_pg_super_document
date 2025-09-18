# OprInfo

## Location
src/bin/pg_dump/pg_dump.h: 259 - 260

## Overview
OprInfo is a structure used in pg_dump to represent operator metadata during database dump operations, containing essential information about PostgreSQL operators including their types, operands, and implementation details.

## Definition


## Detailed Description
OprInfo stores comprehensive metadata about PostgreSQL operators for the dump and restore process. It contains information about the operator's kind (binary, unary left, unary right), the types of its operands, and the function that implements the operator. This structure allows pg_dump to properly reconstruct operator definitions during database restoration, ensuring that custom operators are correctly recreated with their original specifications.

## Parameters / Member Variables
- : DumpableObject containing basic dump metadata (OID, name, namespace, etc.)
- : Name of the role/user who owns this operator
- : Character indicating operator kind - 'b' for binary, 'l' for left unary, 'r' for right unary
- : OID of the left operand type (InvalidOid for right unary operators)
- : OID of the right operand type (InvalidOid for left unary operators)
- : OID of the function that implements this operator

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (base structure for dump metadata)
  - Oid (PostgreSQL object identifier type)

- Called from (representative examples):
  - getOperators (src/bin/pg_dump/pg_dump.c:6024, 6054)
  - dumpOpr (src/bin/pg_dump/pg_dump.c:12962)
  - findOprByOid (src/bin/pg_dump/common.c:934)
  - getFormattedOperatorName (src/bin/pg_dump/pg_dump.c:13224)
  - dumpDumpableObject (src/bin/pg_dump/pg_dump.c:10556)

## Notes and Other Information
- Part of pg_dump's comprehensive type system for handling all PostgreSQL object types
- The oprkind field uses single characters to distinguish between binary operators (taking two operands) and unary operators (taking one operand on left or right)
- InvalidOid is used in oprleft/oprright fields when the operator doesn't take an operand on that side
- Essential for recreating custom operators with correct signatures and implementations during database restoration
- Located in src/bin/pg_dump/pg_dump.h:251-259