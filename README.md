# Day 5 – Advanced Delta Lake Operations

## Objective
To work with advanced Delta Lake features such as incremental MERGE operations, time travel, performance optimization, and data cleanup.

## Dataset Used
- sale_data
- customer

## Tasks Performed
- Implemented incremental MERGE (upserts) on Delta table
- Explored Delta Lake time travel using version history
- Optimized Delta table using OPTIMIZE and ZORDER
- Cleaned old files using VACUUM

## Key Learnings
- MERGE helps handle incremental data updates safely
- Delta Lake supports querying historical versions for audits and debugging
- OPTIMIZE and ZORDER improve query performance
- VACUUM helps manage storage by removing unused files

## Tools Used
- Databricks Community Edition
- Delta Lake
- Apache Spark (PySpark & SQL)

## Notes
This work is part of the Databricks 14 Days AI Challenge organized by Indian Data Club and Codebasics.
