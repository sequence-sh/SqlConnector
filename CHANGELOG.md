# v0.7.0 (2021-03-26)

- Update of Core dependencies only

# v0.6.0 (2021-03-14)

- Update of Core dependencies only

# v0.5.0 (2021-03-01)

## Summary of Changes

First release of the SQL Connector. Versions are aligned with Core.

This connector supports:

- MariaDB
- MS SQL Server
- MySQL
- PostgresSQL

### Steps

- Added
  - CreateConnectionString
  - SqlCreateSchemaFromTable
  - SqlCreateTable
  - SqlInsert
  - SqlQuery
- Added SqlCommand

## Issues Closed in this Release

### New Features

- Add support for postgresql and mariadb #4
- Create Step to insert entity stream into a db table #3
- Create Step to run SQL query and return result as entity, so that technicians can use database data in sequences #1
- Create Step to execute SQL queries that do not return a result #2

### Maintenance

- Update version of Core to support enhanced logging #5
