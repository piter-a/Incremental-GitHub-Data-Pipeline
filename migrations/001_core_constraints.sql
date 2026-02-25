USE incremental_load;

GO

IF NOT EXISTS (
    SELECT 1 
    FROM sys.tables 
    WHERE name = 'schema_migrations'
)
BEGIN
    CREATE TABLE dbo.schema_migrations (
        migration_name VARCHAR(255) PRIMARY KEY,
        applied_at DATETIME2 DEFAULT SYSUTCDATETIME()
    );
END;

SET XACT_ABORT ON;

DECLARE @migration_name VARCHAR(255) = '001_core_constraints';

IF EXISTS (
    SELECT 1 
    FROM dbo.schema_migrations
    WHERE migration_name = @migration_name
)
BEGIN
    PRINT 'Migration already applied: ' + @migration_name;
    RETURN;
END;

BEGIN TRY
    BEGIN TRANSACTION;

    -- Owners unique constraint
    IF NOT EXISTS (
        SELECT 1 
        FROM sys.indexes
        WHERE name = 'UQ_owner_login'
    )
    BEGIN
        ALTER TABLE dbo.owners
        ADD CONSTRAINT UQ_owner_login UNIQUE (owner_login);
    END

    -- Users unique constraint
    IF NOT EXISTS (
        SELECT 1 
        FROM sys.indexes
        WHERE name = 'UQ_user_login'
    )
    BEGIN
        ALTER TABLE dbo.users
        ADD CONSTRAINT UQ_user_login UNIQUE (user_login);
    END

    -- Repos foreign key
    IF NOT EXISTS (
        SELECT 1 
        FROM sys.foreign_keys
        WHERE name = 'FK_repos_owner_id'
    )
    BEGIN
        ALTER TABLE dbo.repos
        ADD CONSTRAINT FK_repos_owner_id FOREIGN KEY (owner_id)
            REFERENCES dbo.owners (owner_id) ON DELETE CASCADE;
    END

    -- Repos unique constraint
    IF NOT EXISTS (
        SELECT 1 
        FROM sys.indexes
        WHERE name = 'UQ_repos_full_name'
    )
    BEGIN
        ALTER TABLE dbo.repos
        ADD CONSTRAINT UQ_repos_full_name UNIQUE (full_name);
    END

    -- Repos check constraints
    IF NOT EXISTS (
        SELECT 1 FROM sys.check_constraints WHERE name = 'CK_repos_visibility'
    )
    BEGIN
        ALTER TABLE dbo.repos
        ADD CONSTRAINT CK_repos_visibility CHECK (visibility IN ('public','private','internal'));
    END

    IF NOT EXISTS (
        SELECT 1 FROM sys.check_constraints WHERE name = 'CK_non_negative_counts'
    )
    BEGIN
        ALTER TABLE dbo.repos
        ADD CONSTRAINT CK_non_negative_counts CHECK (
            stargazers_count >= 0 AND forks_count >= 0 AND watchers_count >= 0 AND open_issues_count >= 0
        );
    END

    IF NOT EXISTS (
        SELECT 1 FROM sys.check_constraints WHERE name = 'CK_repos_timestamps'
    )
    BEGIN
        ALTER TABLE dbo.repos
        ADD CONSTRAINT CK_repos_timestamps CHECK (
            updated_at IS NULL OR updated_at >= created_at
        );
    END

    IF NOT EXISTS (
        SELECT 1 FROM sys.check_constraints WHERE name = 'CK_repos_pushed_at'
    )
    BEGIN
        ALTER TABLE dbo.repos
        ADD CONSTRAINT CK_repos_pushed_at CHECK (
            pushed_at IS NULL OR pushed_at >= created_at
        );
    END

    -- Issues foreign keys
    IF NOT EXISTS (
        SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_issues_author_id'
    )
    BEGIN
        ALTER TABLE dbo.issues
        ADD CONSTRAINT FK_issues_author_id FOREIGN KEY (author_id)
            REFERENCES dbo.users(user_id) ON DELETE CASCADE;
    END

    IF NOT EXISTS (
        SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_issues_assignee_id'
    )
    BEGIN
        ALTER TABLE dbo.issues
        ADD CONSTRAINT FK_issues_assignee_id FOREIGN KEY (assignee_id)
            REFERENCES dbo.users(user_id);
    END

    IF NOT EXISTS (
        SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_issues_repo_id'
    )
    BEGIN
        ALTER TABLE dbo.issues
        ADD CONSTRAINT FK_issues_repo_id FOREIGN KEY (repo_id)
            REFERENCES dbo.repos(repo_id) ON DELETE CASCADE;
    END

    -- Issues check constraints
    IF NOT EXISTS (
        SELECT 1 FROM sys.check_constraints WHERE name = 'CK_issues_timestamps'
    )
    BEGIN
        ALTER TABLE dbo.issues
        ADD CONSTRAINT CK_issues_timestamps CHECK (
            updated_at IS NULL OR updated_at >= created_at
        );
    END

    IF NOT EXISTS (
        SELECT 1 FROM sys.check_constraints WHERE name = 'CK_closed_at'
    )
    BEGIN
        ALTER TABLE dbo.issues
        ADD CONSTRAINT CK_closed_at CHECK (
            closed_at IS NULL OR closed_at >= created_at
        );
    END

    -- Branches foreign key
    IF NOT EXISTS (
        SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_branches_repo_id'
    )
    BEGIN
        ALTER TABLE dbo.branches
        ADD CONSTRAINT FK_branches_repo_id FOREIGN KEY (repo_id)
            REFERENCES dbo.repos(repo_id) ON DELETE CASCADE;
    END

    -- Branches unique constraint
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes WHERE name = 'UQ_repo_branch'
    )
    BEGIN
        ALTER TABLE dbo.branches
        ADD CONSTRAINT UQ_repo_branch UNIQUE (repo_id, branch_name);
    END

    -- Branches check constraint
    IF NOT EXISTS (
        SELECT 1 FROM sys.check_constraints WHERE name = 'CK_commit_sha_hex'
    )
    BEGIN
        ALTER TABLE dbo.branches
        ADD CONSTRAINT CK_commit_sha_hex CHECK (
            commit_sha IS NULL OR commit_sha NOT LIKE '%[^0-9a-fA-F]%'
        );
    END

    -- Record migration
    INSERT INTO dbo.schema_migrations (migration_name)
    VALUES (@migration_name);

    COMMIT TRANSACTION;
    PRINT 'Migration applied: ' + @migration_name;
END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;
    DECLARE @msg NVARCHAR(4000) = ERROR_MESSAGE();
    RAISERROR('Migration failed (%s): %s', 16, 1, @migration_name, @msg);
END CATCH;