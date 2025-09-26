# UserContext

## Location
src/include/utils/usercontext.h: 15 - 20

## Overview
UserContext is a structure that holds the original user context (user ID, security context, and GUC nest level) when temporarily switching to run code as a different database user, allowing for safe restoration of the original state.

## Definition


## Detailed Description
The UserContext structure is a core security mechanism in PostgreSQL that enables temporary privilege escalation or user switching while maintaining the ability to safely restore the original execution context. It is primarily used in conjunction with the  and  functions to implement secure user context switching.

The structure captures three critical pieces of state information:
1. The original user ID before the switch
2. The original security context flags
3. The GUC (Grand Unified Configuration) nesting level for rollback purposes

When switching to an untrusted user (one who cannot SET ROLE back to the original user), the system imposes  restrictions and creates a new GUC nest level to isolate any configuration changes made by the untrusted user's code.

## Parameters / Member Variables
- : The original user ID (Oid) before the user context switch, used to restore the original user identity
- : The original security context flags, including any security restrictions that were in effect
- : The GUC nest level for rolling back configuration changes; set to -1 when no GUC rollback is needed (mutual SET ROLE capability)

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
  - SECURITY_RESTRICTED_OPERATION (security context flag)
  - GUC system (Grand Unified Configuration)
  
- Called from (representative examples):
  - SwitchToUntrustedUser() - initializes the structure with current context
  - RestoreUserContext() - uses the structure to restore original context
  - Table DDL operations (tablecmds.c)
  - Logical replication workers (worker.c)
  - Index operations (indexcmds.c)
  - BRIN index maintenance (brin.c)

## Notes and Other Information
- The structure is always used as a pair with  and  functions
- When  is -1, it indicates that both users can SET ROLE to each other, so no security restrictions or GUC isolation is needed
- When  is >= 0, it indicates that security restrictions were imposed and GUC changes need to be rolled back
- The pattern is commonly used in operations that need to temporarily run as the table/object owner for permission checks or privileged operations
- Critical for maintaining security boundaries when executing user-defined functions or operations that might contain malicious code
- The structure ensures that even if untrusted code attempts to modify the session state, all changes can be safely rolled back
- Used extensively throughout PostgreSQL for operations like index creation, table modifications, and replication tasks where privilege elevation is required