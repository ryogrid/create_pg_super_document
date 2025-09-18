# inet_opr_codenum

## Location
src/backend/utils/adt/network_selfuncs.c: 836 - 878

## Overview
Assigns useful code numbers for the subnet inclusion/overlap operators, providing a standardized numerical representation for inet network operators used in selectivity estimation.

## Definition


## Detailed Description
This function maps PostgreSQL inet operator OIDs to standardized integer codes that are used throughout the network selectivity estimation system. The function implements a simple switch statement that converts operator OIDs into a symmetric code system where:

- Negative codes represent "supernet" operations (contains)  
- Zero represents overlap operations
- Positive codes represent "subnet" operations (contained by)

The code assignment follows a symmetric pattern where negating a code gives the code for the commutator operator, which simplifies logic in other parts of the selectivity estimation system. This design is specifically relied upon by  and .

## Parameters / Member Variables
- : The OID of the inet operator to be converted to a code number

## Dependencies
- Functions called/Symbols referenced:
  - elog (for error reporting)
  - OID_INET_SUP_OP (operator constant)
  - OID_INET_SUPEQ_OP (operator constant)  
  - OID_INET_OVERLAP_OP (operator constant)
  - OID_INET_SUBEQ_OP (operator constant)
  - OID_INET_SUB_OP (operator constant)
- Called from (representative examples):
  - networksel (network selectivity estimation)
  - networkjoinsel_inner (inner join selectivity) 
  - networkjoinsel_semi (semi join selectivity)

## Notes and Other Information
The function returns specific codes:
- -2: Supernet operator (>>)
- -1: Supernet-or-equal operator (>>=)  
- 0: Overlap operator (&&)
- 1: Subnet-or-equal operator (<<=)
- 2: Subnet operator (<<)

The symmetric nature of these codes is crucial for the selectivity estimation algorithms, allowing them to easily determine commutator relationships by simple negation. An error is raised for any unrecognized operator OID.