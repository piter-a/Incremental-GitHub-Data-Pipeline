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

DECLARE @migration_name VARCHAR(255) = '001_core_tables';

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

        -- create prod tables

        IF NOT EXISTS (
            SELECT
                1
            FROM sys.tables t
            WHERE t.[name] = 'dbo.owners'
                AND t.[type] = 'u'
        )

        BEGIN
            CREATE TABLE dbo.owners (
                owner_id UNIQUEIDENTIFIER PRIMARY KEY,
                owner_login VARCHAR (250) NOT NULL
            );
        END;

        IF NOT EXISTS (
            SELECT
                1
            FROM sys.tables t
            WHERE t.[name] = 'dbo.users'
                AND t.[type] = 'u'
        )

        BEGIN
            CREATE TABLE dbo.users (
                [user_id] UNIQUEIDENTIFIER PRIMARY KEY,
                user_login VARCHAR (250) NOT NULL
            );
        END;

        IF NOT EXISTS (
            SELECT
                1
            FROM sys.tables t
            WHERE t.[name] = 'dbo.repos'
                AND t.[type] = 'u'
        )

        BEGIN 
            CREATE TABLE dbo.repos (
                repo_id UNIQUEIDENTIFIER PRIMARY KEY,
                repo_name VARCHAR (200) NOT NULL,
                full_name VARCHAR (255) NOT NULL,
                [description] NVARCHAR (1000) NULL,
                topics NVARCHAR (1000) NULL,
                [language] VARCHAR (100) NULL,
                owner_id UNIQUEIDENTIFIER NOT NULL,
                visibility VARCHAR (50) NOT NULL,
                [private] BIT DEFAULT 0,
                [disabled] BIT DEFAULT 0,
                fork BIT DEFAULT 0,
                archived BIT DEFAULT 0,
                default_branch VARCHAR (255) NULL,
                stargazers_count INT NOT NULL DEFAULT 0,
                watchers_count INT NOT NULL DEFAULT 0,
                forks_count INT NOT NULL DEFAULT 0,
                open_issues_count INT NOT NULL DEFAULT 0,
                created_at DATETIME2 (3) NOT NULL,
                updated_at DATETIME2 (3) NULL,
                pushed_at DATETIME2 (3) NULL
            );
        END;

        IF NOT EXISTS (
            SELECT
                1
            FROM sys.tables t
            WHERE t.[name] = 'dbo.issues'
                AND t.[type] = 'u'
        )

        BEGIN
            CREATE TABLE dbo.issues (
                issue_id UNIQUEIDENTIFIER PRIMARY KEY,
                number INT NOT NULL,
                author_id UNIQUEIDENTIFIER NOT NULL,
                title VARCHAR (250) NOT NULL,
                locked BIT DEFAULT 0,
                comments INT NOT NULL DEFAULT 0,
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
            WHERE t.[name] = 'dbo.branches'
                AND t.[type] = 'u'
        )

        BEGIN
            CREATE TABLE dbo.branches (
                branch_id UNIQUEIDENTIFIER PRIMARY KEY,
                branch_name VARCHAR (250) NOT NULL,
                protected BIT DEFAULT 0,
                commit_sha VARCHAR (40) NULL,
                repo_id UNIQUEIDENTIFIER NOT NULL,
                ingested_at DATETIME2 (3) DEFAULT SYSUTCDATETIME ()
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