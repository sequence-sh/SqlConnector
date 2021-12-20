# Sequence SQL Database Connector

[Reductech Sequence](https://gitlab.com/reductech/sequence) is a collection of
libraries that automates cross-application e-discovery and forensic workflows.

The SQL connector contains Steps to:

- Build connection strings
- Create tables
- Insert entities into tables as rows
- Read entities from tables
- Run arbitrary queries

The following flavors of SQL database are supported:

- MariaDb
- MS SQL Server
- MySQL
- Postgres
- SQLite

## Creating Tables Using Schemas

Schemas are used to create tables and insert entities.

You can create a Schema from an existing table using `SqlCreateSchemaFromTable`

The following schema properties are used in creating tables and inserting entities

| Property               | Description                                        |
| ---------------------- | -------------------------------------------------- |
| `Name`                 | Maps to the name of the SQL table.                 |
| `AllowExtraProperties` | Must be set to `False`                             |
| `Properties`           | Dictionary Mapping column names to column details. |

The following nested properties are also used

| Property       | Description                                                                               |
| -------------- | ----------------------------------------------------------------------------------------- |
| `Type`         | The property type.                                                                        |
| `Multiplicity` | Must be either `UpToOne` for a nullable property or `ExactlyOne` for a not null property. |

This is an example of declaring a schema

```scala
- <Schema> = (
    Name: "MyTable"
    AllowExtraProperties: False
    Properties: (
      Id: (
        Type: SchemaPropertyType.Integer
        Multiplicity: Multiplicity.ExactlyOne
      )
      Name:(
        Type: SchemaPropertyType.String
        Multiplicity: Multiplicity.UpToOne
      )
    )
  )
```

## Example

This is an example of a step that drops a table, recreates it, and inserts an entity.

```scala
- <ConnectionString> = CreateConnectionString
                         Server: "Server"
                         Database: "Database"
                         UserName: "UserName"
                         Password: "Password"
- <Schema> = (
    Name: "MyTable"
    AllowExtraProperties: False
    Properties: (
      Id: (
        Type: SchemaPropertyType.Integer
        Multiplicity: Multiplicity.ExactlyOne
      )
      Name:(
        Type: SchemaPropertyType.String
        Multiplicity: Multiplicity.UpToOne
      )
    )
  )
- SqlCommand
    ConnectionString: <ConnectionString>
    Command: "DROP TABLE IF EXISTS MyTable"
    DatabaseType: 'SQLite'
- SqlCreateTable
    ConnectionString: <ConnectionString>
    Schema: <Schema>
    DatabaseType: 'SQLite'
- SqlInsert
    ConnectionString: <ConnectionString>
    Entities: [
      (Id: 1 Name:'Name1' )
      (Id: 2 Name:'Name2')
    ]
    Schema: <Schema>
    DatabaseType: 'SQLite'
```

## Documentation

- Documentation is available here: https://docs.reductech.io

## E-discovery Reduct

The SQL Connector is part of a group of projects called
[E-discovery Reduct](https://gitlab.com/reductech/sequence
which consists of a collection of [Connectors](https://gitlab.com/reductech/sequence/connectors)
and a command-line application for running Sequences, called
[Sequence](https://gitlab.com/reductech/sequence/console/-/releases).

# Releases

Can be downloaded from the [Releases page](https://gitlab.com/reductech/sequence/connectors/sql/-/releases).

# NuGet Packages

Are available for download from the [Reductech Nuget feed](https://gitlab.com/reductech/nuget/-/packages).
