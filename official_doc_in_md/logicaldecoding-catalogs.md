47.5. System Catalogs Related to Logical Decoding  
---  
[Prev](logicaldecoding-sql.md "47.4. Logical Decoding SQL Interface") | [Up](logicaldecoding.md "Chapter 47. Logical Decoding")| Chapter 47. Logical Decoding| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](logicaldecoding-output-plugin.md "47.6. Logical Decoding Output Plugins")  
  
* * *

## 47.5. System Catalogs Related to Logical Decoding #

The [`pg_replication_slots`](view-pg-replication-slots.md "52.19. pg_replication_slots") view and the [ `pg_stat_replication`](monitoring-stats.md#MONITORING-PG-STAT-REPLICATION-VIEW "27.2.4. pg_stat_replication") view provide information about the current state of replication slots and streaming replication connections respectively. These views apply to both physical and logical replication. The [ `pg_stat_replication_slots`](monitoring-stats.md#MONITORING-PG-STAT-REPLICATION-SLOTS-VIEW "27.2.5. pg_stat_replication_slots") view provides statistics information about the logical replication slots. 

* * *

[Prev](logicaldecoding-sql.md "47.4. Logical Decoding SQL Interface") | [Up](logicaldecoding.md "Chapter 47. Logical Decoding")|  [Next](logicaldecoding-output-plugin.md "47.6. Logical Decoding Output Plugins")  
---|---|---  
47.4. Logical Decoding SQL Interface | [Home](index.md "PostgreSQL 17.5 Documentation")|  47.6. Logical Decoding Output Plugins
