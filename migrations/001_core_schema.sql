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

DECLARE @migration_name VARCHAR(255) = '001_core_schema';

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
            FROM sys.tables t
            WHERE t.[name] = 'owners'
                AND t.[type] = 'u'
        )

        BEGIN
            CREATE TABLE owners (
                owner_id UNIQUEIDENTIFIER PRIMARY KEY,
                owner_login VARCHAR (250) NOT NULL,
                CONSTRAINT UQ_owner_login
                    UNIQUE (owner_login)
            );
        END;

        IF NOT EXISTS (
            SELECT
                1
            FROM sys.tables t
            WHERE t.[name] = 'users'
                AND t.[type] = 'u'
        )

        BEGIN
            CREATE TABLE users (
                [user_id] UNIQUEIDENTIFIER PRIMARY KEY,
                user_login VARCHAR (250) NOT NULL,
                CONSTRAINT UQ_user_login
                    UNIQUE (user_login)
            );
        END;

        IF NOT EXISTS (
            SELECT
                1
            FROM sys.tables t
            WHERE t.[name] = 'repos'
                AND t.[type] = 'u'
        )

        BEGIN 
            CREATE TABLE repos (
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
                pushed_at DATETIME2 (3) NULL,
                CONSTRAINT FK_repos_owner_id
                    FOREIGN KEY (owner_id)
                    REFERENCES owners (owner_id) ON DELETE CASCADE,
                CONSTRAINT UQ_repos_full_name UNIQUE (full_name),
                CONSTRAINT CK_repos_visibility
                    CHECK (visibility IN ('public', 'private', 'internal')),
                CONSTRAINT CK_non_negative_counts
                    CHECK (
                        stargazers_count >= 0
                            AND forks_count >= 0
                            AND watchers_count >= 0
                            AND open_issues_count >= 0),
                CONSTRAINT CK_repos_timestamps
                    CHECK (
                        updated_at IS NULL 
                            OR updated_at >= created_at),
                CONSTRAINT CK_repos_pushed_at
                    CHECK (
                        pushed_at IS NULL 
                            OR pushed_at >= created_at)
            );
        END;

        IF NOT EXISTS (
            SELECT
                1
            FROM sys.tables t
            WHERE t.[name] = 'issues'
                AND t.[type] = 'u'
        )

        BEGIN
            CREATE TABLE issues (
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
                repo_id UNIQUEIDENTIFIER NOT NULL,
                CONSTRAINT FK_issues_author_id
                    FOREIGN KEY (author_id) 
                    REFERENCES users ([user_id]) ON DELETE CASCADE,
                CONSTRAINT FK_issues_assignee_id
                    FOREIGN KEY (assignee_id)
                    REFERENCES users ([user_id]) ON DELETE CASCADE,
                CONSTRAINT FK_issues_repo_id
                    FOREIGN KEY (repo_id)
                    REFERENCES repos (repo_id) ON DELETE CASCADE,
                CONSTRAINT CK_issues_timestamps
                    CHECK (
                        updated_at IS NULL
                            OR updated_at >= created_at),
                CONSTRAINT CK_closed_at
                    CHECK (
                        closed_at IS NULL 
                            OR closed_at >= created_at)
            );
        END;

        IF NOT EXISTS (
            SELECT
                1
            FROM sys.tables t
            WHERE t.[name] = 'branches'
                AND t.[type] = 'u'
        )

        BEGIN
            CREATE TABLE branches (
                branch_id UNIQUEIDENTIFIER PRIMARY KEY,
                branch_name VARCHAR (250) NOT NULL,
                protected BIT DEFAULT 0,
                commit_sha VARCHAR (40) NULL,
                repo_id UNIQUEIDENTIFIER NOT NULL,
                ingested_at DATETIME2 (3) DEFAULT SYSUTCDATETIME (),
                CONSTRAINT FK_branches_repo_id
                    FOREIGN KEY (repo_id)
                    REFERENCES repos (repo_id) ON DELETE CASCADE,
                CONSTRAINT UQ_repo_branch
                    UNIQUE (repo_id, branch_name),
                CONSTRAINT CK_commit_sha_hex
                    CHECK (
                        commit_sha IS NULL
                            OR commit_sha NOT LIKE '%[^0-9a-fA-F]%')
            );
        END;

        IF NOT EXISTS (
            SELECT
                1
            FROM sys.indexes i
            WHERE i.[name] = 'IX_repos_owner_id'
                AND i.[object_id] = OBJECT_ID('repos')
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
                AND i.[object_id] = OBJECT_ID('repos')
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
                AND i.[object_id] = OBJECT_ID('issues')
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
                AND i.[object_id] = OBJECT_ID('issues')
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
                AND i.[object_id] = OBJECT_ID('issues')
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
                AND i.[object_id] = OBJECT_ID('issues')
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
                AND i.[object_id] = OBJECT_ID('branches')
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