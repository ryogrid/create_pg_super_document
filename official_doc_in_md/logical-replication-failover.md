29.3. Logical Replication Failover  
---  
[Prev](logical-replication-subscription.md "29.2. Subscription") | [Up](logical-replication.md "Chapter 29. Logical Replication")| Chapter 29. Logical Replication| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](logical-replication-row-filter.md "29.4. Row Filters")  
  
* * *

## 29.3. Logical Replication Failover #

To allow subscriber nodes to continue replicating data from the publisher node even when the publisher node goes down, there must be a physical standby corresponding to the publisher node. The logical slots on the primary server corresponding to the subscriptions can be synchronized to the standby server by specifying `failover = true` when creating subscriptions. See [Section 47.2.3](logicaldecoding-explanation.md#LOGICALDECODING-REPLICATION-SLOTS-SYNCHRONIZATION "47.2.3. Replication Slot Synchronization") for details. Enabling the [`failover`](sql-createsubscription.md#SQL-CREATESUBSCRIPTION-PARAMS-WITH-FAILOVER) parameter ensures a seamless transition of those subscriptions after the standby is promoted. They can continue subscribing to publications on the new primary server. 

Because the slot synchronization logic copies asynchronously, it is necessary to confirm that replication slots have been synced to the standby server before the failover happens. To ensure a successful failover, the standby server must be ahead of the subscriber. This can be achieved by configuring [`synchronized_standby_slots`](runtime-config-replication.md#GUC-SYNCHRONIZED-STANDBY-SLOTS). 

To confirm that the standby server is indeed ready for failover, follow these steps to verify that all necessary logical replication slots have been synchronized to the standby server: 

  1. On the subscriber node, use the following SQL to identify which replication slots should be synced to the standby that we plan to promote. This query will return the relevant replication slots associated with the failover-enabled subscriptions. 
         
         test_sub=# SELECT
                        array_agg(quote_literal(s.subslotname)) AS slots
                    FROM  pg_subscription s
                    WHERE s.subfailover AND
                          s.subslotname IS NOT NULL;
          slots
         -------
          {'sub1','sub2','sub3'}
         (1 row)
         

  2. On the subscriber node, use the following SQL to identify which table synchronization slots should be synced to the standby that we plan to promote. This query needs to be run on each database that includes the failover-enabled subscription(s). Note that the table sync slot should be synced to the standby server only if the table copy is finished (See [Section 51.55](catalog-pg-subscription-rel.md "51.55. pg_subscription_rel")). We don't need to ensure that the table sync slots are synced in other scenarios as they will either be dropped or re-created on the new primary server in those cases. 
         
         test_sub=# SELECT
                        array_agg(quote_literal(slot_name)) AS slots
                    FROM
                    (
                        SELECT CONCAT('pg_', srsubid, '_sync_', srrelid, '_', ctl.system_identifier) AS slot_name
                        FROM pg_control_system() ctl, pg_subscription_rel r, pg_subscription s
                        WHERE r.srsubstate = 'f' AND s.oid = r.srsubid AND s.subfailover
                    );
          slots
         -------
          {'pg_16394_sync_16385_7394666715149055164'}
         (1 row)
         

  3. Check that the logical replication slots identified above exist on the standby server and are ready for failover. 
         
         test_standby=# SELECT slot_name, (synced AND NOT temporary AND NOT conflicting) AS failover_ready
                        FROM pg_replication_slots
                        WHERE slot_name IN
                            ('sub1','sub2','sub3', 'pg_16394_sync_16385_7394666715149055164');
           slot_name                                 | failover_ready
         --------------------------------------------+----------------
           sub1                                      | t
           sub2                                      | t
           sub3                                      | t
           pg_16394_sync_16385_7394666715149055164   | t
         (4 rows)
         




If all the slots are present on the standby server and the result (`failover_ready`) of the above SQL query is true, then existing subscriptions can continue subscribing to publications now on the new primary server. 

* * *

[Prev](logical-replication-subscription.md "29.2. Subscription") | [Up](logical-replication.md "Chapter 29. Logical Replication")|  [Next](logical-replication-row-filter.md "29.4. Row Filters")  
---|---|---  
29.2. Subscription | [Home](index.md "PostgreSQL 17.5 Documentation")|  29.4. Row Filters
