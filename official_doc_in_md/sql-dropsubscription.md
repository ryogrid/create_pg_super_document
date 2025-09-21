DROP SUBSCRIPTION  
---  
[Prev](sql-dropstatistics.md "DROP STATISTICS") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-droptable.md "DROP TABLE")  
  
* * *

## DROP SUBSCRIPTION

DROP SUBSCRIPTION — remove a subscription

## Synopsis
    
    
    DROP SUBSCRIPTION [ IF EXISTS ] _name_ [ CASCADE | RESTRICT ]
    

## Description

`DROP SUBSCRIPTION` removes a subscription from the database cluster. 

To execute this command the user must be the owner of the subscription. 

`DROP SUBSCRIPTION` cannot be executed inside a transaction block if the subscription is associated with a replication slot. (You can use [`ALTER SUBSCRIPTION`](sql-altersubscription.md "ALTER SUBSCRIPTION") to unset the slot.) 

## Parameters

 _`name`_
    

The name of a subscription to be dropped. 

`CASCADE`  
`RESTRICT`
    

These key words do not have any effect, since there are no dependencies on subscriptions. 

## Notes

When dropping a subscription that is associated with a replication slot on the remote host (the normal state), `DROP SUBSCRIPTION` will connect to the remote host and try to drop the replication slot (and any remaining table synchronization slots) as part of its operation. This is necessary so that the resources allocated for the subscription on the remote host are released. If this fails, either because the remote host is not reachable or because the remote replication slot cannot be dropped or does not exist or never existed, the `DROP SUBSCRIPTION` command will fail. To proceed in this situation, first disable the subscription by executing [ `ALTER SUBSCRIPTION ... DISABLE`](sql-altersubscription.md#SQL-ALTERSUBSCRIPTION-PARAMS-DISABLE), and then disassociate it from the replication slot by executing [ `ALTER SUBSCRIPTION ... SET (slot_name = NONE)`](sql-altersubscription.md#SQL-ALTERSUBSCRIPTION-PARAMS-SET). After that, `DROP SUBSCRIPTION` will no longer attempt any actions on a remote host. Note that if the remote replication slot still exists, it (and any related table synchronization slots) should then be dropped manually; otherwise it/they will continue to reserve WAL and might eventually cause the disk to fill up. See also [Section 29.2.1](logical-replication-subscription.md#LOGICAL-REPLICATION-SUBSCRIPTION-SLOT "29.2.1. Replication Slot Management"). 

If a subscription is associated with a replication slot, then `DROP SUBSCRIPTION` cannot be executed inside a transaction block. 

## Examples

Drop a subscription: 
    
    
    DROP SUBSCRIPTION mysub;
    

## Compatibility

`DROP SUBSCRIPTION` is a PostgreSQL extension. 

## See Also

[CREATE SUBSCRIPTION](sql-createsubscription.md "CREATE SUBSCRIPTION"), [ALTER SUBSCRIPTION](sql-altersubscription.md "ALTER SUBSCRIPTION")

* * *

[Prev](sql-dropstatistics.md "DROP STATISTICS") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-droptable.md "DROP TABLE")  
---|---|---  
DROP STATISTICS | [Home](index.md "PostgreSQL 17.5 Documentation")|  DROP TABLE
