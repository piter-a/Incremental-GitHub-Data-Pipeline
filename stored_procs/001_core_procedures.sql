CREATE OR ALTER PROCEDURE merge_issues 

AS 

    BEGIN TRY
        SET NOCOUNT ON;
        SET XACT_ABORT ON;
    
        MERGE dbo.issues [target] 
        USING stg.issues [source] 
            ON [target].issue_id = [source].issue_id
        
        WHEN MATCHED 
            AND ( 
                [target].updated_at IS NULL 
                    OR [source].updated_at > [target].updated_at 
            ) 

        THEN UPDATE 
            SET 
                number = [source].number, 
                author_id = [source].author_id, 
                title = [source].title, 
                locked = [source].locked, 
                comments = [source].comments, 
                pr_merged_at = [source].pr_merged_at, 
                created_at = [source].created_at, 
                updated_at = [source].updated_at, 
                closed_at = [source].closed_at, 
                labels = [source].labels, 
                assignee_id = [source].assignee_id, 
                repo_id = [source].repo_id 
            
        WHEN NOT MATCHED 
            BY TARGET 
        
        THEN INSERT ( 
            issue_id, 
            number, 
            author_id, 
            title, 
            locked, 
            comments, 
            pr_merged_at, 
            created_at, 
            updated_at, 
            closed_at, 
            labels, 
            assignee_id, 
            repo_id 
        ) 
        
        VALUES ( 
            [source].issue_id, 
            [source].number, 
            [source].author_id, 
            [source].title, 
            [source].locked, 
            [source].comments, 
            [source].pr_merged_at, 
            [source].created_at, 
            [source].updated_at, 
            [source].closed_at, 
            [source].labels, 
            [source].assignee_id, 
            [source].repo_id 
        )

        OUTPUT $action;

    END TRY
    BEGIN CATCH
        THROW
END CATCH;

GO

CREATE OR ALTER PROCEDURE merge_branches 

AS 

    BEGIN TRY
        SET NOCOUNT ON; 
        SET XACT_ABORT ON;
    
        MERGE dbo.branches [target] 
        USING stg.branches [source] 
            ON [target].branch_id = [source].branch_id
        
        WHEN MATCHED 

        THEN UPDATE 
            SET 
                branch_name = [source].branch_name,
                protected = [source].protected,
                commit_sha = [source].commit_sha,
                repo_id = [source].repo_id,
                ingested_at = [source].ingested_at
            
        WHEN NOT MATCHED 
            BY TARGET 
        
        THEN INSERT ( 
            branch_id,
            branch_name,
            protected,
            commit_sha,
            repo_id,
            ingested_at
        ) 
        
        VALUES ( 
            [source].branch_id,
            [source].branch_name,
            [source].protected,
            [source].commit_sha,
            [source].repo_id,
            [source].ingested_at
        )

        OUTPUT $action;

    END TRY
    BEGIN CATCH
        THROW
END CATCH;

GO

CREATE OR ALTER PROCEDURE merge_repos

AS 

    BEGIN TRY
        SET NOCOUNT ON;
        SET XACT_ABORT ON;
    
        MERGE dbo.repos [target] 
        USING stg.repos [source] 
            ON [target].repo_id = [source].repo_id
        
        WHEN MATCHED 
            AND ( 
                [target].updated_at IS NULL 
                    OR [source].updated_at > [target].updated_at 
            ) 

        THEN UPDATE 
            SET 
                repo_name = [source].repo_name,
                full_name = [source].full_name,
                [description] = [source].[description],
                topics = [source].topics,
                [language] = [source].[language],
                owner_id = [source].owner_id,
                visibility = [source].visibility,
                [private] = [source].[private],
                [disabled] = [source].[disabled],
                fork = [source].fork,
                archived = [source].archived,
                default_branch = [source].default_branch,
                stargazers_count = [source].stargazers_count ,
                watchers_count = [source].watchers_count,
                forks_count = [source].forks_count,
                open_issues_count = [source].open_issues_count,
                created_at = [source].created_at,
                updated_at = [source].updated_at,
                pushed_at = [source].pushed_at
            
        WHEN NOT MATCHED 
            BY TARGET 
        
        THEN INSERT ( 
            repo_id,
            repo_name,
            full_name,
            [description],
            topics,
            [language],
            owner_id,
            visibility,
            [private],
            [disabled],
            fork,
            archived,
            default_branch,
            stargazers_count ,
            watchers_count,
            forks_count,
            open_issues_count,
            created_at,
            updated_at,
            pushed_at
        ) 
        
        VALUES ( 
            [source].repo_id,
            [source].repo_name,
            [source].full_name,
            [source].[description],
            [source].topics,
            [source].[language],
            [source].owner_id,
            [source].visibility,
            [source].[private],
            [source].[disabled],
            [source].fork,
            [source].archived,
            [source].default_branch,
            [source].stargazers_count ,
            [source].watchers_count,
            [source].forks_count,
            [source].open_issues_count,
            [source].created_at,
            [source].updated_at,
            [source].pushed_at
        )

        OUTPUT $action;

    END TRY
    BEGIN CATCH
        THROW
END CATCH;

GO

CREATE OR ALTER PROCEDURE merge_users

AS 

    BEGIN TRY
        SET NOCOUNT ON; 
        SET XACT_ABORT ON;
    
        INSERT INTO dbo.users (
            [user_id],
            user_login
        )

        SELECT
            [user_id],
            user_login
        FROM stg.users s
        WHERE NOT EXISTS (
            SELECT
                1
            FROM dbo.users t
            WHERE t.[user_id] = s.[user_id]
        );

    END TRY
    BEGIN CATCH
        THROW
END CATCH;

GO

CREATE OR ALTER PROCEDURE merge_owners

AS 

    BEGIN TRY
        SET NOCOUNT ON; 
        SET XACT_ABORT ON;

        INSERT INTO dbo.owners (
            owner_id,
            owner_login
        )

        SELECT
            owner_id,
            owner_login
        FROM stg.owners s
        WHERE NOT EXISTS (
            SELECT
                1
            FROM dbo.owners t
            WHERE t.owner_id = s.owner_id
        );

    END TRY
    BEGIN CATCH
        THROW
END CATCH;

GO