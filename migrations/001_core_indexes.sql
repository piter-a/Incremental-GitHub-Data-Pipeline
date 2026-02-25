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

DECLARE @migration_name VARCHAR(255) = '001_core_indexes';

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

        IF NOT EXISTS (
            SELECT
                1
            FROM sys.indexes i
            WHERE i.[name] = 'IX_repos_owner_id'
                AND i.[object_id] = OBJECT_ID('dbo.repos')
        )

        BEGIN 
            CREATE INDEX IX_repos_owner_id
                ON repos (owner_id)
        END;

        IF NOT EXISTS (
            SELECT
                1
            FROM sys.indexes i
            WHERE i.[name] = 'IX_repos_created_at'
                AND i.[object_id] = OBJECT_ID('dbo.repos')
        )

        BEGIN
            CREATE INDEX IX_repos_created_at
                ON repos (created_at)
        END;

        IF NOT EXISTS (
            SELECT
                1
            FROM sys.indexes i
            WHERE i.[name] = 'IX_issues_repo_id'
                AND i.[object_id] = OBJECT_ID('dbo.issues')
        )

        BEGIN
            CREATE INDEX IX_issues_repo_id
                ON issues (repo_id)
        END;

        IF NOT EXISTS (
            SELECT
                1
            FROM sys.indexes i
            WHERE i.[name] = 'IX_issues_repo_created'
                AND i.[object_id] = OBJECT_ID('dbo.issues')
        )

        BEGIN 
            CREATE INDEX IX_issues_repo_created
                ON issues (repo_id, created_at)
        END;

        IF NOT EXISTS (
            SELECT
                1
            FROM sys.indexes i
            WHERE i.[name] = 'IX_issues_author_id'
                AND i.[object_id] = OBJECT_ID('dbo.issues')
        )

        BEGIN
            CREATE INDEX IX_issues_author_id
                ON issues (author_id)
        END;

        IF NOT EXISTS (
            SELECT
                1
            FROM sys.indexes i
            WHERE i.[name] = 'IX_issues_assignee_id'
                AND i.[object_id] = OBJECT_ID('dbo.issues')
        )

        BEGIN
            CREATE INDEX IX_issues_assignee_id
                ON issues (assignee_id)
        END;

        IF NOT EXISTS (
            SELECT
                1
            FROM sys.indexes i
            WHERE i.[name] = 'IX_branches_repo_id'
                AND i.[object_id] = OBJECT_ID('dbo.branches')
        )

        BEGIN
            CREATE INDEX IX_branches_repo_id
                ON branches (repo_id)
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