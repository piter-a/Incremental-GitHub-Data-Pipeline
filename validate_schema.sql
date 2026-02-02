USE incremental_load

GO

IF NOT EXISTS (
    SELECT
        t.*
    FROM sys.tables t
    WHERE t.[name] = 'repos'
        AND t.[type] = 'u'
    CREATE TABLE repos (
        );

GO

IF NOT EXISTS (
    SELECT
        t.*
    FROM sys.tables t
    WHERE t.[name] = 'issues'
        AND t.[type] = 'u'
    CREATE TABLE issues (
        );

GO

IF NOT EXISTS (
    SELECT
        t.*
    FROM sys.tables t
    WHERE t.[name] = 'branches'
        AND t.[type] = 'u'
    CREATE TABLE branches (
        );

GO

IF NOT EXISTS (
    SELECT
        t.*
    FROM sys.tables t
    WHERE t.[name] = 'owners'
        AND t.[type] = 'u'
    CREATE TABLE owners (
        owner_id UNIQUEIDENTIFIER PRIMARY KEY,
        owner_login VARCHAR (250) NOT NULL);

GO

IF NOT EXISTS (
    SELECT
        t.*
    FROM sys.tables t
    WHERE t.[name] = 'users'
        AND t.[type] = 'u'
    CREATE TABLE users (
        user_id UNIQUEIDENTIFIER PRIMARY KEY,
        user_login VARCHAR (250) NOT NULL);

GO