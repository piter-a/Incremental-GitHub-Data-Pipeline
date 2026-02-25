USE incremental_load;

GO

IF NOT EXISTS (
    SELECT 
        1 
    FROM sys.tables WHERE name = 'schema_migrations'
)

BEGIN
    CREATE TABLE schema_migrations (
        migration_name VARCHAR(255) PRIMARY KEY,
        applied_at DATETIME2 DEFAULT SYSUTCDATETIME()
    );
END;

SET XACT_ABORT ON;

DECLARE @migration_name VARCHAR(255) = '001_staging_tables';

IF EXISTS (
    SELECT 
        1
    FROM schema_migrations
    WHERE migration_name = @migration_name
)
BEGIN
    PRINT 'Migration already applied: ' + @migration_name;
    RETURN;
END;

BEGIN TRY
    BEGIN TRANSACTION;

        -- create staging tables

        IF NOT EXISTS (
            SELECT
                1
            FROM sys.schemas s
            WHERE s.[name] = 'stg'
        )

        BEGIN 
            EXEC ('CREATE SCHEMA stg');
        END;

        IF NOT EXISTS (
            SELECt
                1
            FROM sys.tables t
            WHERE t.[name] = 'owners'
                AND SCHEMA_NAME (t.schema_id) = 'stg'
        )

        BEGIN
            CREATE TABLE stg.owners (
                owner_id UNIQUEIDENTIFIER PRIMARY KEY,
                owner_login VARCHAR (250) NOT NULL
            );
        END;

        IF NOT EXISTS (
            SELECT
                1
            FROM sys.tables t
            WHERE t.[name] = 'users'
                AND SCHEMA_NAME (t.schema_id) = 'stg'
        )

        BEGIN
            CREATE TABLE stg.users (
                [user_id] UNIQUEIDENTIFIER PRIMARY KEY,
                user_login VARCHAR (250) NOT NULL
            );
        END;

        IF NOT EXISTS (
            SELECT
                1
            FROM sys.tables t
            WHERE t.[name] = 'repos'
                AND SCHEMA_NAME (t.schema_id) = 'stg'
        )

        BEGIN 
            CREATE TABLE stg.repos (
                repo_id UNIQUEIDENTIFIER PRIMARY KEY,
                repo_name VARCHAR (200) NOT NULL,
                full_name VARCHAR (255) NOT NULL,
                [description] NVARCHAR (1000) NULL,
                topics NVARCHAR (1000) NULL,
                [language] VARCHAR (100) NULL,
                owner_id UNIQUEIDENTIFIER NOT NULL,
                visibility VARCHAR (50) NOT NULL,
                [private] BIT,
                [disabled] BIT,
                fork BIT,
                archived BIT,
                default_branch VARCHAR (255) NULL,
                stargazers_count INT NOT NULL,
                watchers_count INT NOT NULL,
                forks_count INT NOT NULL,
                open_issues_count INT NOT NULL,
                created_at DATETIME2 (3) NOT NULL,
                updated_at DATETIME2 (3) NULL,
                pushed_at DATETIME2 (3) NULL
            );
        END;

        IF NOT EXISTS (
            SELECT
                1
            FROM sys.tables t
            WHERE t.[name] = 'issues'
                AND SCHEMA_NAME (t.schema_id) = 'stg'
        )

        BEGIN
            CREATE TABLE stg.issues (
                issue_id UNIQUEIDENTIFIER PRIMARY KEY,
                number INT NOT NULL,
                author_id UNIQUEIDENTIFIER NOT NULL,
                title VARCHAR (250) NOT NULL,
                locked BIT,
                comments INT NOT NULL,
                pr_merged_at DATETIME2 (3) NULL,
                created_at DATETIME2 (3) NOT NULL,
                updated_at DATETIME2 (3) NULL,
                closed_at DATETIME2 (3) NULL,
                labels NVARCHAR (1000) NULL,
                assignee_id UNIQUEIDENTIFIER NULL,
                repo_id UNIQUEIDENTIFIER NOT NULL
            );
        END;

        IF NOT EXISTS (
            SELECT
                1
            FROM sys.tables t
            WHERE t.[name] = 'branches'
                AND SCHEMA_NAME (t.schema_id) = 'stg'
        )

        BEGIN
            CREATE TABLE stg.branches (
                branch_id UNIQUEIDENTIFIER PRIMARY KEY,
                branch_name VARCHAR (250) NOT NULL,
                protected BIT,
                commit_sha VARCHAR (40) NULL,
                repo_id UNIQUEIDENTIFIER NOT NULL,
                ingested_at DATETIME2 (3)
            );
        END;

    INSERT INTO schema_migrations (migration_name)
        VALUES (@migration_name);

    COMMIT TRANSACTION;
    PRINT 'Migration applied: ' + @migration_name;
END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;

    DECLARE @msg NVARCHAR(4000) = ERROR_MESSAGE();
    RAISERROR (
        'Migration failed (%s): %s',
        16, 1,
        @migration_name,
        @msg
    );

END CATCH;