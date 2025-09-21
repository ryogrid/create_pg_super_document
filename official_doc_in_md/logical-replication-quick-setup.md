29.12. Quick Setup  
---  
[Prev](logical-replication-config.md "29.11. Configuration Settings") | [Up](logical-replication.md "Chapter 29. Logical Replication")| Chapter 29. Logical Replication| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](jit.md "Chapter 30. Just-in-Time Compilation \(JIT\)")  
  
* * *

## 29.12. Quick Setup #

First set the configuration options in `postgresql.conf`: 
    
    
    wal_level = logical
    

The other required settings have default values that are sufficient for a basic setup. 

`pg_hba.conf` needs to be adjusted to allow replication (the values here depend on your actual network configuration and user you want to use for connecting): 
    
    
    host     all     repuser     0.0.0.0/0     md5
    

Then on the publisher database: 
    
    
    CREATE PUBLICATION mypub FOR TABLE users, departments;
    

And on the subscriber database: 
    
    
    CREATE SUBSCRIPTION mysub CONNECTION 'dbname=foo host=bar user=repuser' PUBLICATION mypub;
    

The above will start the replication process, which synchronizes the initial table contents of the tables `users` and `departments` and then starts replicating incremental changes to those tables. 

* * *

[Prev](logical-replication-config.md "29.11. Configuration Settings") | [Up](logical-replication.md "Chapter 29. Logical Replication")|  [Next](jit.md "Chapter 30. Just-in-Time Compilation \(JIT\)")  
---|---|---  
29.11. Configuration Settings | [Home](index.md "PostgreSQL 17.5 Documentation")|  Chapter 30. Just-in-Time Compilation (JIT)
