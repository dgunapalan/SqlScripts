-- ******************************************************
-- Change database collation script.
--
-- Created by Igor F. Kovalenko.
-- [skitua@mail.ru], 31.11.2006
-- ******************************************************

-- to run this script in SSMS - please switch to SQLCMD mode.
-- if you want to get script only without any changes in database, please specify @script_only <> 0 

-- please do not use brackets for database name, it should be Northwind not [Northwind]. Names with blanks are not allowed.
:SETVAR destdb PowerHouse_Cypress
:SETVAR desired_collation SQL_Latin1_General_CP1_CI_AS
:SETVAR script_only 0

-- **********************************************************************
-- section one - create stored procedure for temporary usage
-- **********************************************************************
/*
drop table tempdb.dbo.udf
go
drop table tempdb.dbo.udv
go
drop table tempdb.[dbo].[pk_constr]
go
drop table tempdb.[dbo].[pk_constr_cols]
go
drop table tempdb.[dbo].[fk_constr]
go
drop table tempdb.[dbo].[fk_constr_cols]
go
drop table tempdb.[dbo].[fk_constr_cols]
go
drop table tempdb.[dbo].[def_constr]
go
drop table tempdb.dbo.[check_constr]
go
drop table tempdb.[dbo].[comp_cols]
go
drop table tempdb.[dbo].[triggers]
go
drop table tempdb.[dbo].[trigger_order]
go
drop table tempdb.[dbo].[indexes]
go
drop table tempdb.[dbo].[ind_columns]
go
drop table tempdb.[dbo].[permissions]
go 
drop table tempdb.[dbo].[user_stat]
go
drop table tempdb.[dbo].[user_stat_cols]
go
drop table tempdb.[dbo].[bad_tables]
go
drop table tempdb.dbo.[bad_tables]
go
drop table tempdb.[dbo].[bad_table_cols]
go
drop table tempdb.[dbo].[char_cols]
go
drop table tempdb.[dbo].[char_cols]
go 
*/
--Alter Column with lenght max
Declare 	@stmt nvarchar(max) 
Declare Max_cur Cursor for 
select 'Alter Table '+Table_name+ '  Alter  Column ' +column_name +' '+Data_type+ '(2001)' from information_schema.columns
where Character_maximum_length=-1
and table_name like 'tbl%'
Open Max_cur
Fetch next from Max_cur into @stmt
Begin

EXEC sp_executesql @stmt

End
Close Max_cur
DeAllocate Max_cur

USE $(destdb)
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[usp_ANSINULLS_ON_QUOTED_ON]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_ANSINULLS_ON_QUOTED_ON]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[usp_ANSINULLS_ON_QUOTED_ON]
	@stmt nvarchar(max) 
AS

EXEC sp_executesql @stmt

GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[usp_ANSINULLS_ON_QUOTED_OFF]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_ANSINULLS_ON_QUOTED_OFF]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER OFF
GO

CREATE PROCEDURE [dbo].[usp_ANSINULLS_ON_QUOTED_OFF]
	@stmt nvarchar(max) 
AS

EXEC sp_executesql @stmt

GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[usp_ANSINULLS_OFF_QUOTED_ON]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_ANSINULLS_OFF_QUOTED_ON]
GO

SET ANSI_NULLS OFF
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[usp_ANSINULLS_OFF_QUOTED_ON]
	@stmt nvarchar(max) 
AS

SET ANSI_NULLS OFF
SET QUOTED_IDENTIFIER ON 

EXEC sp_executesql @stmt

GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[usp_ANSINULLS_OFF_QUOTED_OFF]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_ANSINULLS_OFF_QUOTED_OFF]
GO

SET ANSI_NULLS OFF
GO
SET QUOTED_IDENTIFIER OFF
GO

CREATE PROCEDURE [dbo].[usp_ANSINULLS_OFF_QUOTED_OFF]
	@stmt nvarchar(max) 
AS

SET ANSI_NULLS OFF
SET QUOTED_IDENTIFIER OFF

EXEC sp_executesql @stmt

GO

-- *************************************************************************
-- end of stored procedures
-- *************************************************************************

-- *************************************************************************
-- set default settings for the script
-- *************************************************************************

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON 
GO

SET NOCOUNT ON

PRINT '--Job start date: ' + cast(getdate() as varchar(40))

PRINT 'USE $(destdb)'
USE $(destdb)

--DBCC CHECKDB
IF $(script_only) <> 0
	PRINT 'DBCC CHECKDB WITH DATA_PURITY'
ELSE
	BEGIN
		PRINT CONVERT(varchar(30), getdate(), 109)
		DBCC CHECKDB WITH DATA_PURITY
		PRINT CONVERT(varchar(30), getdate(), 109)
		PRINT '-- **** CHECKDB FINISHED **** '
		PRINT '-- **** **** '
		PRINT '-- **** **** '
		PRINT '-- **** **** '
	END

-- *********************************************************
-- make some additional checks
-- *********************************************************

DECLARE @RecoveryModel int

SELECT @RecoveryModel = recovery_model 
FROM sys.databases
WHERE [name] = '$(destdb)'

PRINT '-- Database original recovery model - ' + 
	(case @RecoveryModel when 3 then 'SIMPLE' when 2 then 'BULK_LOGGED' when 1 then 'FULL' else 'UNKNOWN' end)

IF @RecoveryModel <> 3 -- SIMPLE
BEGIN 
	PRINT '-- Waiting to change recovery model to SIMPLE...'

	IF $(script_only) = 0
		BEGIN
			ALTER DATABASE $(destdb) SET RECOVERY SIMPLE

			IF @@error <> 0
				BEGIN
					PRINT 'Failed to set recovery model to SIMPLE. Script stopped. Please try again'
					RETURN
				END
			ELSE
				BEGIN
					PRINT 'Recovery model now SIMPLE, please do not forget to restore recovery model if necessary.'
				END
		END
	ELSE
		BEGIN
			PRINT 'ALTER DATABASE $(destdb) SET RECOVERY SIMPLE'
			PRINT 'IF @@error <> 0'
			PRINT '		BEGIN' 
			PRINT '			PRINT ''Failed to set recovery model to SIMPLE. Script stopped. Please try again.'''
			PRINT '			RETURN'
			PRINT '		END'
		END
END

-- *********************************************************
-- declarations
-- *********************************************************

-- variables
DECLARE @has_error int, @num_cycles int, @max_cycles int, @object_id int
SELECT @max_cycles = 10, @num_cycles = 0

DECLARE @old_name varchar(512), @new_name varchar(512)

DECLARE @obj_def nvarchar(max), @obj_name nvarchar(128), @stmt nvarchar(max), @obj_name1 sysname
	, @obj_name2 sysname, @obj_name3 sysname, @flag int, @flag1 int, @mode char(1), @obj_id int
	, @field_list varchar(max), @field varchar(max), @definition varchar(max), @ansi_nulls int
	, @quoted_identifier int

DECLARE @owner sysname, @table_name sysname, @col_name sysname, @length int, @type_name sysname
	, @nullable varchar(8), @precision tinyint, @scale tinyint, @is_user_defined int, @is_identity int
	, @is_computed int, @system_type_id int, @seed int, @increment int

DECLARE @fk_name sysname, @constraint_column_name sysname
	, @referenced_object sysname, @ref_owner sysname, @referenced_column_name sysname
	, @is_disabled int, @is_not_for_replication int, @delete_referential_action int
	, @update_referential_action int, @with_check varchar(15)

DECLARE @fk_name1 sysname, @table_name1 sysname, @owner1 sysname, @constraint_column_name1 sysname
	, @referenced_object1 sysname, @ref_owner1 sysname, @referenced_column_name1 sysname
	, @is_disabled1 int, @is_not_for_replication1 int, @delete_referential_action1 int
	, @update_referential_action1 int, @with_check1 varchar(15)

DECLARE @col_list1 varchar(8000), @col_list2 varchar(8000)

DECLARE	@idx_name sysname, @idx_type tinyint, @idx_type_desc nvarchar(60), @is_unique bit, @ignore_dup_key bit,
	@is_unique_constraint bit, @fill_factor tinyint, @is_padded bit, @allow_row_locks bit,
	@allow_page_locks bit, @key_ordinal tinyint, @is_descending_key bit, @is_included_column bit,
	@fg_name sysname 

DECLARE	@idx_name1 sysname, @idx_type1 tinyint, @idx_type_desc1 nvarchar(60), @is_unique1 bit, @ignore_dup_key1 bit,
	@is_unique_constraint1 bit, @fill_factor1 tinyint, @is_padded1 bit, @allow_row_locks1 bit,
	@allow_page_locks1 bit, @key_ordinal1 tinyint, @is_descending_key1 bit, @is_included_column1 bit,
	@col_name1 sysname, @fg_name1 sysname

-- ******************************************************
-- prepare temporary tables 
-- ******************************************************
PRINT 'USE tempdb'
USE tempdb
PRINT '-- create tempdb tables'

IF $(script_only) = 0
	BEGIN

		-- check constraints
		IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[check_constr]') AND type in (N'U'))
		DROP TABLE [dbo].[check_constr]

		-- default constraints
		IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[def_constr]') AND type in (N'U'))
		DROP TABLE [dbo].[def_constr]

		-- foreign key constraints
		IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[fk_constr]') AND type in (N'U'))
		DROP TABLE [dbo].[fk_constr]

		-- foreign key constraints - columns description
		IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[fk_constr_cols]') AND type in (N'U'))
		DROP TABLE [dbo].[fk_constr_cols]

		-- user defined function
		IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[udf]') AND type in (N'U'))
		DROP TABLE [dbo].[udf]

		-- views
		IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[udv]') AND type in (N'U'))
		DROP TABLE [dbo].[udv]

		-- computed columns
		IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[comp_cols]') AND type in (N'U'))
		DROP TABLE [dbo].[comp_cols]

		-- char columns - candidate for collation change
		IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[char_cols]') AND type in (N'U'))
		DROP TABLE [dbo].[char_cols]

		-- indexes 
		IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[indexes]') AND type in (N'U'))
		DROP TABLE [dbo].[indexes]

		-- columns from indexes
		IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ind_columns]') AND type in (N'U'))
		DROP TABLE [dbo].[ind_columns]

		-- primary keys
		IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[pk_constr]') AND type in (N'U'))
		DROP TABLE [dbo].[pk_constr]

		-- primary keys columns
		IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[pk_constr_cols]') AND type in (N'U'))
		DROP TABLE [dbo].[pk_constr_cols]

		-- triggers
		IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[triggers]') AND type in (N'U'))
		DROP TABLE [dbo].[triggers]

		-- trigger_order
		IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[trigger_order]') AND type in (N'U'))
		DROP TABLE [dbo].[trigger_order]

		-- permissions
		IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[permissions]') AND type in (N'U'))
		DROP TABLE [dbo].[permissions]

		-- user-created statistics
		IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[user_stat]') AND type in (N'U'))
		DROP TABLE [dbo].[user_stat]

		-- user-created statistics & columns
		IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[user_stat_cols]') AND type in (N'U'))
		DROP TABLE [dbo].[user_stat_cols]

		-- bad tables - to restore computed columns
		IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[bad_tables]') AND type in (N'U'))
		DROP TABLE [dbo].[bad_tables]

		IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[bad_table_cols]') AND type in (N'U'))
		DROP TABLE [dbo].[bad_table_cols]

	END

PRINT '-- LOCK DATABASE'
PRINT '-- ***'

-- lock database
PRINT 'USE $(destdb)'
USE $(destdb)

IF $(script_only) = 0
	BEGIN
		ALTER DATABASE $(destdb) SET single_user WITH ROLLBACK IMMEDIATE

		IF @@error <> 0 
		BEGIN
			PRINT 'Could not alter database to sinle_use mode'
			RAISERROR('Could not alter database to sinle_use mode', 16, 1)
			RETURN
		END
	END
ELSE 
	BEGIN
		PRINT 'ALTER DATABASE $(destdb) SET single_user WITH ROLLBACK IMMEDIATE'
		PRINT 'IF @@error <> 0 '
		PRINT '		BEGIN'
		PRINT '			PRINT ''Could not alter database to sinle_use mode'''
		PRINT '			RAISERROR(''Could not alter database to sinle_use mode'', 16, 1)'
		PRINT '			RETURN'
		PRINT '		END'
	END

-- ******************************************************
-- ******************************************************
-- ******************************************************
-- ******************************************************
-- ******************************************************
-- ******************************************************
-- ******************************************************
-- ******************************************************
-- ******************************************************
-- gathering information
-- ******************************************************
-- ******************************************************
-- ******************************************************
-- ******************************************************
-- ******************************************************
-- ******************************************************
-- ******************************************************
-- ******************************************************
-- ******************************************************

PRINT '--Collect info section start time: ' + cast(getdate() as varchar(40))


-- **************************************************
-- udf info
-- **************************************************
CREATE TABLE tempdb.dbo.udf(
	[object_id] [int],
	[udf_name] [sysname],
	[owner] [sysname],
	[definition] [nvarchar](max),
	[dropped] [int],
	[ansi_nulls] [int],
	[quoted_identifier] [int]
)

PRINT '-- GET UDF INFO (both schema-bound and uses_database_collation)'

INSERT INTO tempdb.dbo.udf 
	([object_id], [udf_name], [owner], [definition], [ansi_nulls], [quoted_identifier])
SELECT o.[object_id]
	, [udf_name] = o.[name]
	, owner = s.[name]
	, m.[definition]
	, [ansi_nulls] = m.uses_ansi_nulls
	, [quoted_identifier] = m.uses_quoted_identifier
FROM sys.sql_modules m
	JOIN sys.objects o on o.[object_id] = m.[object_id]
	join sys.schemas s on s.schema_id = o.schema_id 
WHERE (
	m.uses_database_collation = 1
		or
		m.is_schema_bound = 1
	)
	and o.[type] in ('TF', 'IF', 'FN')

IF @@error <> 0 
BEGIN
	PRINT '-- UDF INFO FAILED. SEE ERROR LOG FOR DETAILS.'
	RAISERROR('UDF INFO FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
	RETURN
END

-- **************************************************
-- view info
-- **************************************************
CREATE TABLE tempdb.dbo.udv(
	[object_id] [int],
	[udv_name] [sysname],
	[owner] [sysname],
	[definition] [nvarchar](max),
	[dropped] [int],
	[ansi_nulls] [int],
	[quoted_identifier] [int]
)

PRINT '-- GET VIEW INFO (both schema-bound and uses_database_collation)'

INSERT INTO tempdb.dbo.udv
	([object_id], [udv_name], [owner], [definition], [ansi_nulls], [quoted_identifier])
SELECT o.[object_id]
	, [udv_name] = o.[name]
	, owner = s.[name]
	, m.[definition]
	, [ansi_nulls] = m.uses_ansi_nulls
	, [quoted_identifier] = m.uses_quoted_identifier
FROM sys.sql_modules m
	JOIN sys.objects o on o.[object_id] = m.[object_id]
	join sys.schemas s on s.schema_id = o.schema_id 
WHERE (
	m.uses_database_collation = 1
		or
		m.is_schema_bound = 1
	)
	and o.[type] = 'V'

IF @@error <> 0 
BEGIN
	PRINT '-- VIEW INFO FAILED. SEE ERROR LOG FOR DETAILS.'
	RAISERROR('VIEW INFO FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
	RETURN
END


-- *************************************************
-- primary key constraints info
-- *************************************************
CREATE TABLE tempdb.[dbo].[pk_constr](
	[table_name] [sysname] ,
	[owner] [sysname] ,
	[idx_name] [sysname] 
)

CREATE TABLE tempdb.[dbo].[pk_constr_cols](
	[table_name] [sysname],
	[owner] [sysname],
	[idx_name] [sysname],
	[idx_type] [tinyint],
	[idx_type_desc] [nvarchar](60),
	[is_unique] [bit],
	[ignore_dup_key] [bit],
	[is_unique_constraint] [bit],
	[fill_factor] [tinyint],
	[is_padded] [bit],
	[is_disabled] [bit],
	[allow_row_locks] [bit],
	[allow_page_locks] [bit],
	[col_name] [sysname],
	[key_ordinal] [tinyint],
	[is_descending_key] [bit],
	[is_included_column] [bit],
	[fg_name] [sysname] 
)

PRINT '-- GET PK INFO'

INSERT INTO tempdb.[dbo].[pk_constr]
SELECT [table_name] = o.[name]
	, owner = s.[name]
	, [idx_name] = kc.[name] 
FROM sys.key_constraints kc
	join sys.objects o on o.[object_id] = kc.[parent_object_id]
	join sys.schemas s on s.schema_id = o.schema_id
WHERE kc.[type] = 'PK'

IF @@error <> 0 
BEGIN
	PRINT '-- PK INFO FAILED. SEE ERROR LOG FOR DETAILS.'
	RAISERROR('PK INFO FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
	RETURN
END


INSERT INTO tempdb.[dbo].[pk_constr_cols]
SELECT [table_name] = o.[name]
		, owner = s.[name]
		, idx_name = kc.[name]
		, idx_type = i.[type]
		, idx_type_desc = i.type_desc
		, i.is_unique
		, i.ignore_dup_key
		, i.is_unique_constraint
		, i.fill_factor
		, i.is_padded
		, i.is_disabled
		, i.allow_row_locks
		, i.allow_page_locks
		, [col_name] = c.[name]
		, ic.key_ordinal
		, ic.is_descending_key
		, ic.is_included_column
		, [fg_name] = ds.[name]
FROM sys.key_constraints kc
	JOIN sys.objects o on o.[object_id] = kc.[parent_object_id]
	JOIN sys.schemas s on s.schema_id = o.schema_id
	JOIN sys.indexes i on i.[object_id] = kc.[parent_object_id] AND i.index_id = kc.unique_index_id
	LEFT JOIN sys.index_columns ic on ic.[object_id] = i.[object_id] and ic.index_id = i.index_id
	LEFT JOIN sys.columns c on c.[object_id] = ic.[object_id] and c.column_id = ic.column_id
	JOIN sys.data_spaces ds on ds.data_space_id = i.data_space_id
WHERE kc.[type] = 'PK'
ORDER BY o.[name], s.[name], i.[name], ic.key_ordinal

IF @@error <> 0 
BEGIN
	PRINT '-- PK COLUMNS INFO FAILED. SEE ERROR LOG FOR DETAILS.'
	RAISERROR('PK COLUMNS INFO FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
	RETURN
END


-- *************************************************
-- foreign key constraints info
-- *************************************************
CREATE TABLE tempdb.[dbo].[fk_constr](
	[table_name] [sysname] ,
	[owner] [sysname] ,
	[fk_constr] [sysname] ,
	[referenced_table] [sysname] ,
	[referenced_table_owner] [sysname] ,
	[update_referential_action] [tinyint] ,
	[delete_referential_action] [tinyint] ,
	[is_not_for_replication] [bit] ,
	[is_disabled] [bit] ,
	[const_id] [int]
)

CREATE TABLE tempdb.[dbo].[fk_constr_cols](
	[foreign_key_name] [sysname],
	[table_name] [sysname],
	[owner] [sysname],
	[constraint_column_name] [nvarchar](128),
	[referenced_object] [sysname],
	[ref_owner] [sysname],
	[referenced_column_name] [nvarchar](128),
	[is_disabled] [bit],
	[is_not_for_replication] [bit],
	[delete_referential_action] [tinyint] ,
	[update_referential_action] [tinyint] ,	
	[constraint_column_id] [int],
	[with_check] [varchar](12)
)

PRINT '-- GET FOREIGN KEY CONSTRAINTS INFO'

INSERT INTO tempdb.[dbo].[fk_constr]
SELECT [table_name] = ot.[name]
		, [owner] = st.[name]
		, [fk_constr] = f.[name]
		, [referenced_table] = otr.[name]
		, [referenced_table_owner] = st1.[name]
		, f.[update_referential_action]
		, f.[delete_referential_action]
		, f.[is_not_for_replication]
		, f.[is_disabled]
		, [const_id] = f.[object_id]
FROM sys.foreign_keys f
	join sys.objects ot on ot.[object_id] = f.parent_object_id
	join sys.schemas st on st.schema_id = ot.schema_id
	join sys.objects otr on otr.[object_id] = f.referenced_object_id
	join sys.schemas st1 on st1.schema_id = otr.schema_id

IF @@error <> 0 
BEGIN
	PRINT '-- FOREIGN KEY INFO FAILED. SEE ERROR LOG FOR DETAILS.'
	RAISERROR('FOREIGN KEY INFO FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
	RETURN
END


PRINT '-- GET FOREIGN KEY CONSTRAINTS & COLUMNS INFO'
INSERT INTO tempdb.[dbo].[fk_constr_cols]
SELECT 
	f.[name] AS foreign_key_name
	, ot.[name] AS table_name
	, st.[name] AS owner
	, COL_NAME(fc.parent_object_id, fc.parent_column_id) AS constraint_column_name
	, otr.[name] AS referenced_object
	, st1.[name] AS ref_owner
	, COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS referenced_column_name
	, f.is_disabled	
	, f.[is_not_for_replication]
	, f.delete_referential_action--_desc
	, f.update_referential_action--_desc
	, fc.constraint_column_id
	, [with_check] = case f.is_not_trusted when 1 then 'WITH NOCHECK' else '' end
FROM sys.foreign_keys AS f
	JOIN sys.foreign_key_columns AS fc ON f.[object_id] = fc.constraint_object_id 
	join sys.objects ot on ot.[object_id] = f.parent_object_id
	join sys.schemas st on st.schema_id = ot.schema_id
	join sys.objects otr on otr.[object_id] = f.referenced_object_id
	join sys.schemas st1 on st1.schema_id = otr.schema_id
	JOIN tempdb.dbo.[fk_constr] tfc on tfc.[const_id] = fc.constraint_object_id 
order by f.[name], fc.constraint_column_id

IF @@error <> 0 
BEGIN
	PRINT '-- FOREIGN KEY COLUMNS INFO FAILED. SEE ERROR LOG FOR DETAILS.'
	RAISERROR('FOREIGN KEY COLUMNS INFO FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
	RETURN
END


-- *************************************************
-- default constraints
-- *************************************************
CREATE TABLE tempdb.[dbo].[def_constr](
	[table_name] [sysname] ,
	[owner] [sysname] ,
	[default_name] [sysname] ,
	[col_name] [sysname] ,
	[definition] [nvarchar](max)
) 

PRINT '-- GET DEFAULT CONSTRAINTS INFO'

INSERT INTO tempdb.[dbo].[def_constr]
SELECT [table_name] = o.[name]
		, [owner] = s.[name]
		, [default_name] = dc.[name]
		, [col_name] = c.[name]
		, dc.definition
FROM sys.default_constraints dc
	join sys.columns c on c.[object_id] = dc.parent_object_id and c.column_id = dc.parent_column_id
	join sys.objects o on o.[object_id] = dc.parent_object_id
	join sys.schemas s on s.schema_id = o.schema_id
-- this selection criteria shold be. But we will drop and recreate all constaints because of 
--WHERE c.system_type_id in (35, 99, 167, 175, 231, 239, 256)

IF @@error <> 0 
BEGIN
	PRINT '-- DEFAULT CONSTRAINTS INFO FAILED. SEE ERROR LOG FOR DETAILS.'
	RAISERROR('DEFAULT CONSTRAINTS INFO FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
	RETURN
END


-- *************************************************
-- check constraints
-- *************************************************
CREATE TABLE tempdb.dbo.[check_constr](
	[name] [sysname],
	[par_obj_name] [sysname],
	[owner] [sysname],
	[definition] [nvarchar](max),
	[with_check] varchar(12)
)

PRINT '-- GET CHECK CONSTRAINTS INFO'

INSERT INTO tempdb.dbo.[check_constr]
SELECT cc.[name]
	, par_obj_name = o.[name]
	, owner = s.[name]
	, cc.definition
	, [with_check] = case cc.is_not_trusted when 1 then 'WITH NOCHECK' else '' end
FROM sys.check_constraints cc
	join sys.objects o on o.[object_id] = cc.parent_object_id
	join sys.schemas s on s.schema_id = o.schema_id

IF @@error <> 0 
BEGIN
	PRINT '-- CHECK CONSTRAINTS INFO FAILED. SEE ERROR LOG FOR DETAILS.'
	RAISERROR('CHECK CONSTRAINTS INFO FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
	RETURN
END


-- ************************************************************************
-- computed columns
-- ************************************************************************
CREATE TABLE tempdb.[dbo].[comp_cols](
	[table_name] [sysname],
	[owner] [sysname],
	[col_name] [sysname],
	[definition] [nvarchar](max),
	[object_id] [int],
	[column_id] [int], 
	[drop_safe] [int]
)

PRINT '-- GET COMPUTED COLUMNS INFO'

INSERT INTO tempdb.[dbo].[comp_cols]
SELECT table_name = o.[name]
	, owner = s.[name]
	, [col_name] = c.[name]
	, c.definition
	, c.[object_id]
	, c.column_id
	, drop_safe = isnull(b.recreate_manually, 0)
FROM sys.computed_columns c
	join sys.types t on t.user_type_id = c.user_type_id
	join sys.objects o on o.[object_id] = c.[object_id]
	join sys.schemas s on s.schema_id = o.schema_id
	left join (
		SELECT DISTINCT a.table_object_id, a.table_name
			, recreate_manually = case when a.recreate_manually > 0 then 1 else 0 end
		FROM (
				select [table_name] = o.[name]
					, [table_object_id] = cc.[object_id]
					, column_name = cc.[name]
					, cc.column_id 
					, recreate_manually = ISNULL(
											(SELECT TOP 1 c.column_id FROM sys.columns c 
											WHERE c.[object_id] = cc.[object_id]
												AND c.column_id > cc.column_id
												AND c.is_computed = 0 )
											, 0)
				from sys.computed_columns cc 
					join sys.objects o on o.[object_id] = cc.[object_id]

				) a
			WHERE a.recreate_manually > 0
			) b on b.table_object_id = o.[object_id]
WHERE (c.system_type_id in (35, 99, 167, 175, 231, 239, 256)
		or 
		c.uses_database_collation = 1)
	and o.[type] = 'U'
ORDER BY s.[name], o.[name], c.column_id

IF @@error <> 0 
BEGIN
	PRINT '-- GET COMPUTED COLUMN INFO FAILED. SEE ERROR LOG FOR DETAILS.'
	RAISERROR('GET COMPUTED COLUMN INFO FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
	RETURN
END


-- ***********************************************************************
-- triggers
-- ***********************************************************************
CREATE TABLE tempdb.[dbo].[triggers](
	[trigger_name] [sysname],
	[table_name] [sysname],
	[owner] [sysname],
	[is_disabled] [bit],
	[is_not_for_replication] [bit],
	[is_instead_of_trigger] [bit],
	[definition] [nvarchar](max),
	[ansi_nulls] int,
	[quoted_identifier] int
) 

PRINT '-- GET TRIGGER INFO'

INSERT INTO tempdb.[dbo].[triggers]
SELECT trigger_name = t.[name] 
		, table_name = o.[name]
		, owner = s.[name]
		, t.is_disabled
		, t.is_not_for_replication
		, t.is_instead_of_trigger
		, m.definition
		, [ansi_nulls] = m.uses_ansi_nulls 
		, [quoted_identifier] = m.uses_quoted_identifier 
FROM sys.triggers t
	join sys.sql_modules m on m.[object_id] = t.[object_id]
	JOIN sys.objects o on o.[object_id] = t.parent_id
	JOIN sys.schemas s on s.schema_id = o.schema_id
WHERE t.parent_class = 1 -- only table & view triggers
	and t.[type] = 'TR'

IF @@error <> 0 
BEGIN
	PRINT '-- GET TRIGGER INFO FAILED. SEE ERROR LOG FOR DETAILS.'
	RAISERROR('TRIGGER INFO FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
	RETURN
END


CREATE TABLE tempdb.[dbo].[trigger_order](
	[trigger_name] [sysname],
	[owner] [sysname],
	[type_desc] [nvarchar](60),
	[action] [varchar](5)
) 

PRINT '-- GET TRIGGER ORDER INFO'

INSERT INTO tempdb.[dbo].[trigger_order]
SELECT trigger_name = o.[name]
	, owner = s.[name]
	, te.[type_desc]
	, [action] = cast('First' as varchar(5))
--into a1
FROM sys.trigger_events te
	JOIN sys.objects o on o.[object_id] = te.[object_id]
	JOIN sys.schemas s on s.schema_id = o.schema_id
	JOIN sys.triggers t on t.[object_id] = te.[object_id]
WHERE te.is_first = 1
	and t.parent_class = 1 -- only table & view triggers
	and t.[type] = 'TR'

UNION ALL

SELECT trigger_name = o.[name]
	, owner = s.[name]
	, te.[type_desc]
	, [action] = cast('Last' as varchar(5))
FROM sys.trigger_events te
	JOIN sys.objects o on o.[object_id] = te.[object_id]
	JOIN sys.schemas s on s.schema_id = o.schema_id
	JOIN sys.triggers t on t.[object_id] = te.[object_id]
WHERE te.is_last = 1
	and t.parent_class = 1 -- only table & view triggers
	and t.[type] = 'TR'

IF @@error <> 0 
BEGIN
	PRINT '-- GET TRIGGER ORDER INFO FAILED. SEE ERROR LOG FOR DETAILS.'
	RAISERROR('TRIGGER ORDER INFO FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
	RETURN
END

-- ***********************************************************************
-- indexes - without primary keys
-- ***********************************************************************
CREATE TABLE tempdb.[dbo].[indexes](
	[table_name] [sysname] ,
	[owner] [sysname] ,
	[idx_name] [sysname] ,
	[is_unique_constraint] [bit]
) 

CREATE TABLE tempdb.[dbo].[ind_columns](
	[table_name] [sysname],
	[owner] [sysname],
	[idx_name] [sysname],
	[idx_type] [tinyint],
	[idx_type_desc] [nvarchar](60) ,
	[is_unique] [bit] ,
	[ignore_dup_key] [bit] ,
	[is_unique_constraint] [bit] ,
	[fill_factor] [tinyint] ,
	[is_padded] [bit] ,
	[is_disabled] [bit] ,
	[allow_row_locks] [bit] ,
	[allow_page_locks] [bit] ,
	[col_name] [sysname] ,
	[key_ordinal] [tinyint] ,
	[is_descending_key] [bit] ,
	[is_included_column] [bit] ,
	[fg_name] [sysname]
) 

PRINT '-- GET INDEXES INFO'

INSERT INTO tempdb.[dbo].[indexes]
SELECT [table_name] = o.[name]
		, owner = s.[name]
		, idx_name = i.[name]
		, i.is_unique_constraint
FROM sys.indexes AS i
	JOIN sys.objects o on o.[object_id] = i.[object_id]
	JOIN sys.schemas s on s.schema_id = o.schema_id
WHERE o.[type] = 'U'
	and i.is_primary_key = 0
	and i.[type] <> 0

IF @@error <> 0 
BEGIN
	PRINT '-- GET INDEX INFO FAILED. SEE ERROR LOG FOR DETAILS.'
	RAISERROR('INDEX INFO FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
	RETURN
END


INSERT INTO tempdb.[dbo].[ind_columns]
SELECT [table_name] = o.[name]
		, owner = s.[name]
		, idx_name = i.[name]
		, idx_type = i.[type]
		, idx_type_desc = i.type_desc
		, i.is_unique
		, i.ignore_dup_key
		, i.is_unique_constraint
		, i.fill_factor
		, i.is_padded
		, i.is_disabled
		, i.allow_row_locks
		, i.allow_page_locks
		, [col_name] = c.[name]
		, ic.key_ordinal
		, ic.is_descending_key
		, ic.is_included_column
		, [fg_name] = ds.[name]
FROM sys.indexes i
	JOIN sys.objects o on o.[object_id] = i.[object_id]
	JOIN sys.schemas s on s.schema_id = o.schema_id
	left join sys.index_columns ic on ic.[object_id] = i.[object_id] and ic.index_id = i.index_id
	LEFT JOIN sys.columns c on c.[object_id] = ic.[object_id] and c.column_id = ic.column_id
	join sys.data_spaces ds on ds.data_space_id = i.data_space_id
WHERE o.[type] = 'U'
	and i.is_primary_key = 0
	and i.[type] <> 0
ORDER BY o.[name], s.[name], i.[name], ic.key_ordinal

IF @@error <> 0 
BEGIN
	PRINT '-- GET INDEX COLUMNS INFO FAILED. SEE ERROR LOG FOR DETAILS.'
	RAISERROR('INDEX COLUMNS INFO FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
	RETURN
END


-- ***********************************************************************
-- permissions for UDF & calculated columns & tables to delete/recreate
-- ***********************************************************************
CREATE TABLE tempdb.[dbo].[permissions](
	[object_id] [int],
	[state] [char](1),
	[state_desc] [nvarchar](60),
	[permission_name] [nvarchar](128),
	[owner] [sysname],
	[obj_name] [sysname],
	[principal_name] [sysname],
	[col_name] [sysname]
) 

PRINT '-- GET PERMISSIONS INFO'

INSERT INTO tempdb.[dbo].[permissions]
SELECT o.[object_id]
		, dp.state
		, dp.state_desc
		, dp.permission_name
		, [owner] = s.[name]
		, [obj_name] = o.[name]
		, [principal_name] = p.[name]
		, [col_name] = isnull(c.[name], '')
FROM  sys.database_permissions dp
	join sys.database_principals p on p.principal_id = dp.grantee_principal_id
	JOIN sys.objects o on dp.major_id = o.[object_id]
	join sys.schemas s on o.schema_id = s.schema_id
	left join sys.columns c on c.column_id = dp.minor_id and c.[object_id] = dp.major_id
WHERE major_id in --(1189579276, 78623323)
	(
		SELECT [object_id] FROM tempdb.dbo.udf
		UNION ALL
		SELECT [object_id] FROM tempdb.dbo.udv
		UNION ALL
		SELECT [object_id] FROM tempdb.dbo.[comp_cols] 
	)
	
IF @@error <> 0 
BEGIN
	PRINT '-- PERMISSIONS INFO FAILED. SEE ERROR LOG FOR DETAILS.'
	RAISERROR('PERMISSIONS INFO FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
	RETURN
END


-- ***********************************************************************
-- user-managed statistics
-- ***********************************************************************
CREATE TABLE tempdb.[dbo].[user_stat](
	[table_name] [sysname] ,
	[owner] [sysname] ,
	[stat_name] [sysname] ,
	[no_recompute] [bit]
)

PRINT '-- GET USER STATISTICS INFO'

INSERT INTO tempdb.[dbo].[user_stat]
SELECT [table_name] = o.[name]
	, owner = sc.[name]
	, [stat_name] = s.[name]
	, s.no_recompute
FROM sys.stats s 
	join sys.objects o on o.[object_id] = s.[object_id]
	join sys.schemas sc on sc.schema_id = o.schema_id
WHERE s.user_created = 1

IF @@error <> 0 
BEGIN
	PRINT '-- USER STATISTICS INFO FAILED. SEE ERROR LOG FOR DETAILS.'
	RAISERROR('USER STATISTICS INFO FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
	RETURN
END

PRINT '-- GET USER STATISTICS INFO WITH COLUMNS'

CREATE TABLE tempdb.[dbo].[user_stat_cols](
	[table_name] [sysname],
	[owner] [sysname],
	[stat_name] [sysname],
	[col_order] [int],
	[no_recompute] [bit],
	[column_name] [sysname]
) 

INSERT INTO tempdb.[dbo].[user_stat_cols]
SELECT [table_name] = o.[name]
	, owner = sch.[name]
	, [stat_name] = s.[name]
	, [col_order] = sc.stats_column_id
	, s.no_recompute
	, column_name = c.[name]
FROM sys.stats s 
	join sys.objects o on o.[object_id] = s.[object_id]
	join sys.schemas sch on sch.schema_id = o.schema_id
	join sys.stats_columns sc on sc.[object_id] = s.[object_id] and sc.stats_id = s.stats_id
	join sys.columns c on c.[object_id] = sc.[object_id] and c.column_id = sc.column_id
WHERE s.user_created = 1
ORDER BY sch.[name], o.[name], s.[name], sc.stats_column_id

IF @@error <> 0 
BEGIN
	PRINT '-- USER STATISTICS INFO WITH COLUMNS FAILED. SEE ERROR LOG FOR DETAILS.'
	RAISERROR('USER STATISTICS INFO WITH COLUMNS FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
	RETURN
END

PRINT '--'
PRINT '--Collect info section finish time: ' + cast(getdate() as varchar(40))
PRINT '--'


-- ****************************************************************
-- ****************************************************************
-- ****************************************************************
-- ****************************************************************
-- ****************************************************************
-- ****************************************************************
-- ****************************************************************
-- delete section 
-- ****************************************************************
-- ****************************************************************
-- ****************************************************************
-- ****************************************************************
-- ****************************************************************
-- ****************************************************************
-- ****************************************************************
-- ****************************************************************

PRINT '--'
PRINT '--DROP info section start time: ' + cast(getdate() as varchar(40))
PRINT '--'


-- ****************************************************************
-- drop udf
-- ****************************************************************

PRINT '-- ***'
PRINT '-- DROP UDF'

SET @num_cycles = 0

TRY_DELETE_AGAIN:

SET @has_error = 0

IF NOT EXISTS(SELECT TOP 1 1 FROM tempdb.dbo.udf WHERE isnull([dropped], 0) = 0)
	GOTO SKIP_DROP_UDF

DECLARE ms_func_cursor CURSOR FOR
SELECT [object_id], udf_name, owner 
FROM tempdb.dbo.udf
WHERE isnull([dropped], 0) = 0

-- open cursor
open ms_func_cursor
fetch next from ms_func_cursor into @object_id, @table_name, @owner

WHILE @@fetch_status >= 0
BEGIN
	SET @stmt = 'IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N''[' + @owner + '].[' + 
			@table_name + ']'') AND type in (N''FN'', N''IF'', N''TF'', N''FS'', N''FT''))' + CHAR(13) + CHAR(10) + 
			'DROP FUNCTION [' + @owner + '].[' + @table_name + ']'

	PRINT @stmt

	IF $(script_only) = 0
		EXEC(@stmt)

	IF @@error <> 0 
		BEGIN
			PRINT '-- DROP FUNCTION FAILED. Wait for a next cycle.'
			-- RAISERROR('DROP FUNCTION FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
			-- RETURN
			SET @has_error = 1
		END
	ELSE
		BEGIN
			PRINT '-- DROP FUNCTION SUCCEED'
			UPDATE tempdb.dbo.udf SET [dropped] = 1 WHERE [object_id] = @object_id
			
			IF @@error <> 0 
				BEGIN
					PRINT '-- update temp table when DROP FUNCTION FAILED. SEE ERROR LOG FOR DETAILS.'
					RAISERROR('update temp table when DROP FUNCTION FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
					RETURN
				END
		END

	fetch next from ms_func_cursor into @object_id, @table_name, @owner
END

-- close cursor
CLOSE ms_func_cursor
DEALLOCATE ms_func_cursor

SKIP_DROP_UDF:

IF @has_error = 0
	PRINT '-- DROP UDF passed.'

-- ****************************************************************
-- drop udv
-- ****************************************************************

PRINT '-- ***'
PRINT '-- DROP VIEWS'

IF NOT EXISTS(SELECT TOP 1 1 FROM tempdb.dbo.udv WHERE isnull([dropped], 0) = 0)
	GOTO SKIP_DROP_UDV

DECLARE ms_view_cursor CURSOR FOR
SELECT [object_id], udv_name, owner 
FROM tempdb.dbo.udv
WHERE isnull([dropped], 0) = 0

-- open cursor
open ms_view_cursor
fetch next from ms_view_cursor into @object_id, @table_name, @owner

WHILE @@fetch_status >= 0
BEGIN
	SET @stmt = 'IF  EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N''[' + @owner + '].[' + 
			@table_name + ']''))' + CHAR(13) + CHAR(10) + 
			'DROP VIEW [' + @owner + '].[' + @table_name + ']'

	PRINT @stmt

	IF $(script_only) = 0
		EXEC(@stmt)

	IF @@error <> 0 
		BEGIN
			PRINT '-- DROP VIEW FAILED. Wait for a next cycle.'
			-- RAISERROR('DROP VIEW FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
			-- RETURN
			SET @has_error = 1
		END
	ELSE
		BEGIN
			PRINT '-- DROP VIEW SUCCEED'
			UPDATE tempdb.dbo.udv SET [dropped] = 1 WHERE [object_id] = @object_id
			
			IF @@error <> 0 
				BEGIN
					PRINT '-- update temp table when DROP VIEW FAILED. SEE ERROR LOG FOR DETAILS.'
					RAISERROR('update temp table when DROP VIEW FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
					RETURN
				END
		END

	fetch next from ms_view_cursor into @object_id, @table_name, @owner
END

-- close cursor
CLOSE ms_view_cursor
DEALLOCATE ms_view_cursor

SKIP_DROP_UDV:

IF @has_error = 1
	BEGIN
		SET @num_cycles = @num_cycles + 1
		IF @num_cycles > @max_cycles
			BEGIN
				PRINT '-- maximum count of attempts when DROP VIEW is reached. SEE ERROR LOG FOR DETAILS.'
				RAISERROR('maximum count of attempts when DROP VIEW is reached. SEE ERROR LOG FOR DETAILS.', 16, 1)
				RETURN
			END
		ELSE
			BEGIN
				PRINT '-- trying to delete binding objects again...(' + cast(@num_cycles as varchar(3)) + ')'
				GOTO TRY_DELETE_AGAIN
			END
	END

PRINT '-- DROP VIEW passed.'


-- ****************************************************************
-- drop triggers
-- ****************************************************************
PRINT '-- ***'
PRINT '-- DROP TRIGGERS'

IF NOT EXISTS(SELECT TOP 1 1 FROM tempdb.dbo.[triggers])
	GOTO SKIP_DROP_TRIGGERS

DECLARE ms_trig_cursor CURSOR FOR
SELECT [trigger_name], [table_name], [owner] FROM tempdb.dbo.[triggers]

-- open cursor
open ms_trig_cursor
fetch next from ms_trig_cursor into @obj_name, @table_name, @owner

WHILE @@fetch_status >= 0
BEGIN
--	SET @stmt = 'ALTER TABLE [' + @owner + '].[' + @table_name + '] DISABLE TRIGGER [' + @obj_name + ']'
	SET @stmt = 'DROP TRIGGER [' + @obj_name + ']'
	PRINT @stmt

	IF $(script_only) = 0
		EXEC(@stmt)

	IF @@error <> 0 
	BEGIN
		PRINT '--DROP TRIGGER FAILED. SEE ERROR LOG FOR DETAILS.'
		RAISERROR('DROP TRIGGER FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
		RETURN
	END

	fetch next from ms_trig_cursor into @obj_name, @table_name, @owner
END

-- close cursor
CLOSE ms_trig_cursor
DEALLOCATE ms_trig_cursor

SKIP_DROP_TRIGGERS:

PRINT '-- DROP TRIGGERS PASSED.'

-- ****************************************************************
-- drop statistics
-- ****************************************************************
PRINT '-- ***'
PRINT '-- DROP HAND-MADE STATISTICS'

IF NOT EXISTS(SELECT TOP 1 1 FROM tempdb.dbo.[user_stat])
	GOTO SKIP_DROP_USER_STAT

DECLARE ms_stat_cursor CURSOR FOR
SELECT owner, table_name, stat_name FROM tempdb.dbo.[user_stat]

open ms_stat_cursor
fetch next from ms_stat_cursor into @owner, @table_name, @obj_name

WHILE @@fetch_status >= 0
BEGIN

	--DROP STATISTICS [dbo].[CERTIFICATES].[Stat_certificates]

	SET @stmt = 'DROP STATISTICS [' + @owner + '].[' + @table_name + '].[' + @obj_name + ']'
	PRINT @stmt

	IF $(script_only) = 0
		EXEC(@stmt)

	IF @@error <> 0 
	BEGIN
		PRINT '-- DROP STATISTICS FAILED. SEE ERROR LOG FOR DETAILS.'
		RAISERROR('DROP STATISTICS FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
		RETURN
	END

	fetch next from ms_stat_cursor into  @owner, @table_name, @obj_name
END

-- close cursor
CLOSE ms_stat_cursor
DEALLOCATE ms_stat_cursor

SKIP_DROP_USER_STAT:

PRINT '-- DROP HAND-MADE STATISTICS passed'

-- ****************************************************************
-- delete foreign key constraints
-- ****************************************************************
PRINT '-- ***'
PRINT '-- DROP FK'

IF NOT EXISTS(SELECT TOP 1 1 FROM tempdb.dbo.[fk_constr])
	GOTO SKIP_DROP_FK

DECLARE ms_fk_cursor CURSOR FOR
SELECT owner, table_name, fk_constr FROM tempdb.dbo.[fk_constr]

open ms_fk_cursor

fetch next from ms_fk_cursor into @owner, @table_name, @obj_name

WHILE @@fetch_status >= 0
BEGIN
	SET @stmt = 'ALTER TABLE [' + @owner + '].[' + @table_name + '] DROP CONSTRAINT [' + @obj_name + ']'
	PRINT @stmt

	IF $(script_only) = 0
		EXEC(@stmt)

	IF @@error <> 0 
	BEGIN
		PRINT '-- DROP FK FAILED. SEE ERROR LOG FOR DETAILS.'
		RAISERROR('DROP FK FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
		RETURN
	END

	fetch next from ms_fk_cursor into @owner, @table_name, @obj_name
END

-- close cursor
CLOSE ms_fk_cursor
DEALLOCATE ms_fk_cursor

SKIP_DROP_FK:

PRINT '-- DROP FK passed'


-- ****************************************************************
-- delete default constraints
-- ****************************************************************
PRINT '-- ***'
PRINT '-- DROP DEFAULT CONSTRAINTS'

IF NOT EXISTS(SELECT TOP 1 1 FROM tempdb.dbo.[def_constr])
	GOTO SKIP_DROP_DEFAULTS

DECLARE ms_dc_cursor CURSOR FOR
SELECT [table_name], [owner], default_name FROM tempdb.dbo.[def_constr]

open ms_dc_cursor
fetch next from ms_dc_cursor into @table_name, @owner, @obj_name

WHILE @@fetch_status >= 0
BEGIN
	SET @stmt = 'ALTER TABLE [' + @owner + '].[' + @table_name + '] DROP CONSTRAINT [' + @obj_name + ']'
	PRINT @stmt

	IF $(script_only) = 0
		EXEC(@stmt)

	IF @@error <> 0 
	BEGIN
		PRINT '-- DROP DEFAULT CONSTRAINT FAILED. SEE ERROR LOG FOR DETAILS.'
		RAISERROR('DROP DROP DEFAULT CONSTRAINT FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
		RETURN
	END

	fetch next from ms_dc_cursor into @table_name, @owner, @obj_name
END

-- close cursor
CLOSE ms_dc_cursor
DEALLOCATE ms_dc_cursor

SKIP_DROP_DEFAULTS:

PRINT '-- DROP DEFAULT CONSTRAINTS passed'

-- ****************************************************************
-- drop check constraints
-- ****************************************************************
PRINT '-- ***'
PRINT '-- DROP CHECK CONSTRAINTS'

IF NOT EXISTS(SELECT TOP 1 1 FROM tempdb.dbo.[check_constr])
	GOTO SKIP_DROP_CHECK_CONSTR

DECLARE ms_cc_cursor CURSOR FOR
SELECT [name], [owner], par_obj_name FROM tempdb.dbo.[check_constr]

open ms_cc_cursor
fetch next from ms_cc_cursor into @obj_name, @owner, @table_name

WHILE @@fetch_status >= 0
BEGIN
	SET @stmt = 'ALTER TABLE [' + @owner + '].[' + @table_name + '] DROP CONSTRAINT [' + @obj_name + ']'
	PRINT @stmt

	IF $(script_only) = 0
		EXEC(@stmt)

	IF @@error <> 0 
	BEGIN
		PRINT '-- DROP CHECK CONSTRAINT FAILED. SEE ERROR LOG FOR DETAILS.'
		RAISERROR('DROP CHECK CONSTRAINT FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
		RETURN
	END

	fetch next from ms_cc_cursor into @obj_name, @owner, @table_name
END

-- close cursor
CLOSE ms_cc_cursor
DEALLOCATE ms_cc_cursor

SKIP_DROP_CHECK_CONSTR:

PRINT '-- DROP CHECK CONSTRAINTS failed'

-- ****************************************************************
-- drop indexes
-- ****************************************************************
PRINT '-- ***'
PRINT '-- DROP INDEXES'

IF NOT EXISTS(SELECT TOP 1 1 FROM tempdb.dbo.indexes)
	GOTO SKIP_DROP_INDEXES

DECLARE ms_ind_cursor CURSOR FOR
SELECT table_name, [owner], [idx_name], is_unique_constraint FROM tempdb.dbo.indexes

open ms_ind_cursor
fetch next from ms_ind_cursor into @table_name, @owner, @obj_name, @flag

WHILE @@fetch_status >= 0
BEGIN
	IF @flag = 1
		BEGIN
			--ALTER TABLE dbo.ccc DROP CONSTRAINT ccc_id2_unique
			SET @stmt = 'ALTER TABLE [' + @owner + '].[' + @table_name + '] DROP CONSTRAINT [' + @obj_name + ']'
		END
	ELSE
		BEGIN
			--DROP INDEX IX_bbb_bbbtext ON dbo.bbb
			SET @stmt = 'DROP INDEX [' + @obj_name + '] ON [' + @owner + '].[' + @table_name + ']'
		END
	PRINT @stmt

	IF $(script_only) = 0
		EXEC(@stmt)

	IF @@error <> 0 
	BEGIN
		PRINT '-- DROP INDEX (OR CONSTRAINT) FAILED. SEE ERROR LOG FOR DETAILS.'
		RAISERROR('DROP INDEX (OR CONSTRAINT) FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
		RETURN
	END

	fetch next from ms_ind_cursor into @table_name, @owner, @obj_name, @flag
END

-- close cursor
CLOSE ms_ind_cursor
DEALLOCATE ms_ind_cursor

SKIP_DROP_INDEXES:

PRINT '-- DROP INDEXES passes'

-- ****************************************************************
-- drop primary keys
-- ****************************************************************
PRINT '-- ***'
PRINT '-- DROP PK'

IF NOT EXISTS(SELECT TOP 1 1 FROM tempdb.dbo.[pk_constr])
	GOTO SKIP_DROP_PK

DECLARE ms_pk_cursor CURSOR FOR
SELECT table_name, [owner], [idx_name] FROM tempdb.dbo.[pk_constr]

open ms_pk_cursor
fetch next from ms_pk_cursor into @table_name, @owner, @obj_name

WHILE @@fetch_status >= 0
BEGIN
	--ALTER TABLE dbo.ccc DROP CONSTRAINT ccc_id2_unique
	SET @stmt = 'ALTER TABLE [' + @owner + '].[' + @table_name + '] DROP CONSTRAINT [' + @obj_name + ']'
	PRINT @stmt

	IF $(script_only) = 0
		EXEC(@stmt)

	IF @@error <> 0 
	BEGIN
		PRINT '--DROP PK FAILED. SEE ERROR LOG FOR DETAILS.'
		RAISERROR('DROP PK FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
		RETURN
	END

	fetch next from ms_pk_cursor into @table_name, @owner, @obj_name
END

-- close cursor
CLOSE ms_pk_cursor
DEALLOCATE ms_pk_cursor

SKIP_DROP_PK:

PRINT '-- DROP PK passed'


-- ****************************************************************
-- alter computed columns
-- ****************************************************************
PRINT '-- ***'
PRINT '-- DROP COMPUTED COLUMNS'

-- only "good columns"
IF NOT EXISTS(SELECT TOP 1 1 FROM tempdb.dbo.[comp_cols] WHERE drop_safe = 0)
	GOTO SKIP_DROP_COMP_COLS

DECLARE ms_comp_col_cursor CURSOR FOR
SELECT table_name, owner, [col_name] 
FROM tempdb.dbo.[comp_cols]
WHERE drop_safe = 0

open ms_comp_col_cursor
fetch next from ms_comp_col_cursor into @table_name, @owner, @obj_name

WHILE @@fetch_status >= 0
BEGIN
	SET @stmt = 'ALTER TABLE [' + @owner + '].[' + @table_name + '] DROP COLUMN [' + @obj_name + ']'
	PRINT @stmt

	IF $(script_only) = 0
		EXEC(@stmt)

	IF @@error <> 0 
	BEGIN
		PRINT '-- ALTER COMPUTED COLUMN FAILED. SEE ERROR LOG FOR DETAILS.'
		RAISERROR('ALTER COMPUTED COLUMN FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
		RETURN
	END

	fetch next from ms_comp_col_cursor into @table_name, @owner, @obj_name
END

-- close cursor
CLOSE ms_comp_col_cursor
DEALLOCATE ms_comp_col_cursor

SKIP_DROP_COMP_COLS:

PRINT '-- DROP COMPUTED COLUMNS passed.'


-- ***************************************************************
-- creating script for "bad" tables with computed columns
-- ***************************************************************

PRINT '-- ***'
PRINT '-- GET BAD TABLES INFO'

CREATE TABLE tempdb.[dbo].[bad_tables](
	[object_id] [int] NOT NULL,
	[table_name] [sysname],
	[owner] [sysname],
	[file_group] [sysname]
)

INSERT INTO tempdb.dbo.[bad_tables]
SELECT DISTINCT o.[object_id]
	, [table_name] = o.[name]
	, [owner] = s.[name]
	, [file_group] = d.[name]
FROM sys.objects o 
	JOIN tempdb.dbo.comp_cols c on c.[object_id] = o.[object_id]
	JOIN sys.schemas s on s.schema_id = o.schema_id
	JOIN sys.indexes i on i.[object_id] = o.[object_id]
	JOIN sys.data_spaces d on d.data_space_id = i.data_space_id
where o.[type] = 'U'-- [name] = 'a'
	and i.index_id < 2
	and c.[drop_safe] <> 0

IF @@error <> 0 
BEGIN
	PRINT '-- GET BAD TABLES INFO FAILED. SEE ERROR LOG FOR DETAILS.'
	RAISERROR('GET BAD TABLES INFO FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
	RETURN
END


CREATE TABLE tempdb.[dbo].[bad_table_cols](
	[object_id] [int] NOT NULL,
	[col_name] [sysname],
	[column_id] [int] NOT NULL,
	[length] [int] NULL,
	[precision] [tinyint] NOT NULL,
	[scale] [tinyint] NOT NULL,
	[type_name] [sysname],
	[nullable] [varchar](8),
	[is_user_defined] [bit] NOT NULL,
	[is_identity] [bit] NOT NULL,
	[is_computed] [bit] NOT NULL,
	[system_type_id] [tinyint] NOT NULL,
	[seed_value] [int] NULL,
	[increment_value] [int] NULL,
	[definition] [nvarchar](max)
) 

PRINT '-- GET BAD TABLE COLUMNS INFO'

INSERT INTO tempdb.dbo.[bad_table_cols]
SELECT c.[object_id]
	, [col_name] = c.[name]
	, c.column_id 
	, length = case when t.[name] like 'n%' then c.max_length / 2  else c.max_length end
	, [precision] = c.[precision]
	, c.scale
	, type_name = t.[name]
	, nullable = case when c.is_nullable=0 then 'NOT ' else '' end + 'NULL'
	, t.is_user_defined
	, c.is_identity
	, c.is_computed
	, [system_type_id] = c.user_type_id -- system_type_id
	, seed_value = cast(i.seed_value as int)
	, increment_value = cast(i.increment_value as int)
	, [definition] = case c.is_computed when 0 then '' else cc.definition end
FROM sys.columns c
	LEFT JOIN tempdb.dbo.comp_cols cc on cc.[object_id] = c.[object_id] and cc.[column_id] = c.[column_id]
	JOIN (SELECT DISTINCT [object_id], [drop_safe] FROM tempdb.dbo.comp_cols) c1 on c1.[object_id] = c.[object_id]
	JOIN sys.types t on t.system_type_id = c.system_type_id and t.user_type_id = c.user_type_id
	LEFT JOIN sys.identity_columns i on c.[object_id] = i.[object_id] and c.column_id = i.column_id
WHERE c1.[drop_safe] <> 0
--	and c.[object_id] = 2099048
ORDER BY c.[object_id], c.column_id

IF @@error <> 0 
BEGIN
	PRINT '-- GET BAD TABLE COLUMNS INFO FAILED. SEE ERROR LOG FOR DETAILS.'
	RAISERROR('GET BAD TABLE COLUMNS INFO FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
	RETURN
END

PRINT '--'
PRINT '--Special section for copying table start time: ' + cast(getdate() as varchar(40))
PRINT '--'


-- ***************************************************************
-- now copy table to shadow
-- ***************************************************************
PRINT '-- ***'
PRINT '-- COPY TABLES FOR COMPUTED COLUMNS'

-- only "good columns"
IF NOT EXISTS(SELECT TOP 1 1 FROM tempdb.dbo.[bad_tables])
	GOTO SKIP_COPY_COMP_COLS

DECLARE ms_comp_col_cursor CURSOR FOR
SELECT [object_id], table_name, owner, file_group
FROM tempdb.dbo.[bad_tables]

open ms_comp_col_cursor
fetch next from ms_comp_col_cursor into @obj_id, @table_name, @owner, @obj_name

WHILE @@fetch_status >= 0
BEGIN

	PRINT '-- COPYING TABLE ' + @owner + '.' + @table_name + '....'

	-- prepare filed list
	SET @field_list = ''

	SELECT @field_list = '[' + [col_name] + '],' + @field_list
	FROM tempdb.dbo.bad_table_cols
	WHERE [object_id] = @obj_id --1781581385
		and is_computed = 0
	ORDER BY column_id

	SET @field_list = left(@field_list, len(@field_list)-1)

	-- print @flist

	-- check if table exists
	IF  EXISTS (SELECT * FROM tempdb.sys.objects 
				WHERE [object_id] = OBJECT_ID('tempdb.[' + @owner + '].[a1234___' + @table_name + ']') AND type in (N'U'))
		BEGIN
			SET @stmt = 'DROP TABLE tempdb.[' + @owner + '].[a1234___' + @table_name + ']'
			PRINT @stmt
			
			IF $(script_only) = 0
				EXEC(@stmt)

			IF @@error <> 0 
				BEGIN
					PRINT '-- DROP EXISTING TABLE FOR COMPUTED COLUMN FAILED. SEE ERROR LOG FOR DETAILS.'
					RAISERROR('DROP EXISTING TABLE FOR COMPUTED COLUMN FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
					RETURN
				END

		END
	ELSE
		PRINT '-- TABLE tempdb.[' + @owner + '].[a1234___' + @table_name + '] does not exists'

	-- create temp table
	SET @stmt = 'SELECT ' + @field_list COLLATE $(desired_collation) + ' INTO tempdb.[' 
						+ @owner COLLATE $(desired_collation) + '].[a1234___' 
						+ @table_name COLLATE $(desired_collation) + '] FROM [' 
						+ @owner COLLATE $(desired_collation) + '].[' 
						+ @table_name COLLATE $(desired_collation) + ']'
	PRINT @stmt

	IF $(script_only) = 0
		EXEC(@stmt)

	IF @@error <> 0 
	BEGIN
		PRINT '-- COPY TABLE FOR COMPUTED COLUMN FAILED. SEE ERROR LOG FOR DETAILS.'
		RAISERROR('COPY TABLE FOR COMPUTED COLUMN FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
		RETURN
	END

	-- drop original table
	SET @stmt = 'DROP TABLE [' + @owner + '].[' + @table_name + ']'
	PRINT @stmt

	IF $(script_only) = 0
		EXEC(@stmt)

	IF @@error <> 0 
	BEGIN
		PRINT '-- FAILED TO DROP TABLE FOR COMPUTED COLUMN. SEE ERROR LOG FOR DETAILS.'
		RAISERROR('FAILED TO DROP TABLE FOR COMPUTED COLUMN. SEE ERROR LOG FOR DETAILS.', 16, 1)
		RETURN
	END

	fetch next from ms_comp_col_cursor into @obj_id, @table_name, @owner, @obj_name
END

-- close cursor
CLOSE ms_comp_col_cursor
DEALLOCATE ms_comp_col_cursor

SKIP_COPY_COMP_COLS:

PRINT '-- COPY TABLES FOR COMPUTED COLUMNS passed'

-- ***********************************************************************
-- select all character columns here because we are doing workaround 
-- for computed columns
-- ***********************************************************************
CREATE TABLE tempdb.[dbo].[char_cols](
	[col_id] [int] NOT NULL,
	[owner] [sysname] ,
	[table_name] [sysname],
	[col_name] [sysname],
	[length] [int],
	[type_name] [sysname],
	[nullable] [varchar](8),
	[is_user_defined] [bit]
)

PRINT '-- GET CHAR COLUMNS INFO'

INSERT INTO tempdb.[dbo].[char_cols]
SELECT  col_id = c.[object_id]
		, [owner] = s.[name]
		, [table_name] = o.[name]
		, [col_name] = c.[name]
		, length = case when t.[name] like 'n%' then c.max_length / 2  else c.max_length end
		, type_name = t.[name]
		, nullable = case when c.is_nullable=0 then 'NOT ' else '' end + 'NULL'
		, t.is_user_defined
--		, c.*, o.*, t.*
FROM sys.columns c
	join sys.objects o on o.[object_id] = c.[object_id]
	join sys.types t on t.system_type_id = c.system_type_id and t.user_type_id = c.user_type_id
	join sys.schemas s on s.schema_id = o.schema_id
WHERE c.system_type_id in (35, 99, 167, 175, 231, 239, 256)
	and o.[type] = 'U'
	and c.is_computed = 0

IF @@error <> 0 
BEGIN
	PRINT '-- CHAR COLUMNS INFO FAILED. SEE ERROR LOG FOR DETAILS.'
	RAISERROR('CHAR COLUMNS INFO FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
	RETURN
END

PRINT '--'
PRINT '--DROP section finish time: ' + cast(getdate() as varchar(40))
PRINT '--'







PRINT '--'
PRINT '--Change collation section start time: ' + cast(getdate() as varchar(40))
PRINT '--'





























/*

-- ****************************************************************
-- start changing collation
-- ****************************************************************
PRINT '-- ******************'
PRINT '-- TRY TO CHANGE COLLATION....'

-- start disabling constraints
PRINT 'ALTER DATABASE $(destdb) COLLATE $(desired_collation)'
ALTER DATABASE $(destdb) COLLATE $(desired_collation)

IF @@error <> 0 
BEGIN
	PRINT '-- ALTER DATABASE COLLATION FAILED, PLEASE CHECK THE REASON, ATTACH DB AND RUN AGAIN.'
	RAISERROR('ALTER DATABASE COLLATION FAILED, PLEASE CHECK THE REASON, ATTACH DB AND RUN AGAIN.', 16, 1)
	RETURN
END

PRINT '-- ***'
PRINT '-- DB COLLATION HAS BEEN CHANGED SUCCESSFULLY'

PRINT '-- ***'
PRINT '-- COLUMNS COLLATION IN PROGRESS...'

IF NOT EXISTS(SELECT TOP 1 1 FROM tempdb.dbo.char_cols)
	GOTO SKIP_DROP_CHAR_COLS

DECLARE ms_col_cursor CURSOR FOR
select owner, table_name, [col_name], length, type_name, nullable, is_user_defined
from tempdb.dbo.char_cols
where length<>-1

open ms_col_cursor
fetch next from ms_col_cursor into @owner, @table_name, @col_name, @length, @type_name, @nullable, @flag

WHILE @@fetch_status >= 0
BEGIN
	SET @stmt = 'ALTER TABLE [' + @owner + '].[' + @table_name + '] ALTER COLUMN [' + @col_name + '] ' 
		+ @type_name 

	-- length
	IF NOT (UPPER(@type_name COLLATE DATABASE_DEFAULT) LIKE '%TEXT' OR 
			UPPER(@type_name COLLATE DATABASE_DEFAULT) = 'SYSNAME' OR 
			@flag = 1)
		SET @stmt = @stmt + '(' + cast(@length as varchar(4)) + ')' 

	IF @flag = 0
		SET @stmt = @stmt + ' COLLATE DATABASE_DEFAULT ' 
 
	SET @stmt = @stmt + ' ' + @nullable
	
	PRINT @stmt

	IF $(script_only) = 0
		EXEC(@stmt)

	IF @@error <> 0 
	BEGIN
		PRINT '-- ALTER COLUMN FAILED. SEE ERROR LOG FOR DETAILS.'
		RAISERROR('ALTER COLUMN FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
		RETURN
	END

	fetch next from ms_col_cursor into @owner, @table_name, @col_name, @length, @type_name, @nullable, @flag
END

-- close cursor
CLOSE ms_col_cursor
DEALLOCATE ms_col_cursor

SKIP_DROP_CHAR_COLS:

PRINT '-- ***'
PRINT '-- CHANGING COLUMNS COLLATION - done!'

PRINT '--'
PRINT '--Change collation section end time: ' + cast(getdate() as varchar(40))
PRINT '--'
*/
drop table tblmembers

CREATE TABLE [dbo].[tblmembers](
	[vcMemberID] [varchar](12) NOT NULL,
	[vcVisibleMemberID] [varchar](12) NOT NULL,
	[vcBarcodeID] [varchar](25) NOT NULL,
	[vcSalutation] [varchar](10) NULL,
	[vcFirstName] [varchar](50) NULL,
	[vcMiddleName] [varchar](50) NULL,
	[vcLastName] [varchar](50) NULL,
	[vcSuffix] [varchar](5) NULL,
	[vcPreferredName] [varchar](50) NULL,
	[vcCurrentFacilityID] [varchar](6) NULL,
	[cGender] [char](1) NULL,
	[sdtDateOfBirth] [datetime] NULL,
	[vcState] [varchar](50) NULL,
	[vcCity] [varchar](50) NULL,
	[vcZip] [varchar](20) NULL,
	[vcCountry] [varchar](50) NULL,
	[vcHomePhone] [varchar](25) NULL,
	[vcWorkPhone] [varchar](25) NULL,
	[vcWorkPhoneExtn] [varchar](5) NULL,
	[vcEmail] [varchar](100) NULL,
	[vcPager] [varchar](25) NULL,
	[vcMobile] [varchar](25) NULL,
	[siStatusID] [smallint] NOT NULL,
	[sdtLastClubVisit] [datetime] NULL,
	[cActiveFlag] [char](1) NOT NULL,
	[vcCreatedBy] [varchar](12) NULL,
	[sdtCreatedDate] [datetime] NOT NULL,
	[vcModifiedBy] [varchar](12) NULL,
	[sdtModifiedDate] [datetime] NULL,
	[vcCreatedTerminalID] [varchar](25) NULL,
	[vcModifiedTerminalID] [varchar](25) NULL,
	[vcBillID] [varchar](12) NULL,
	[cSponsorType] [char](1) NOT NULL,
	[vcAddress1] [varchar](100) NULL,
	[vcAddress2] [varchar](100) NULL,
	[vcSalesPersonEmpID] [varchar](12) NULL,
	[iMembershipContractid] [int] NULL,
	[cIsContractOwner] [char](1) NOT NULL,
	[IsCorporateMember] [char](1) NOT NULL,
	[cMemberCategory] [char](1) NOT NULL,
	[iHoldMembershipContractId] [int] NULL,
	[iProspectClass] [smallint] NULL,
	[dtProspectExpiryDate] [datetime] NULL,
	[siProspectStatusID] [smallint] NULL,
	[sdtMemberSince] [smalldatetime] NULL,
	[vctempVisibleMemberID] [varchar](12) NULL,
	[vctempBarCodeID] [varchar](25) NULL,
	[vcRelation] [varchar](50) NULL,
	[cGroupMember] [char](1) NULL,
	[cIsGroupAccount] [char](1) NOT NULL,
	[cUseSponsorCard] [char](1) NOT NULL,
	[sdtFirstJoinDate] [smalldatetime] NULL,
 
)

insert into tblmembers
select * from temp_tblmembers


-- **************************************************
-- **************************************************
-- **************************************************
-- **************************************************
-- **************************************************
-- **************************************************
-- **************************************************
-- **************************************************
-- recreate section
-- **************************************************
-- **************************************************
-- **************************************************
-- **************************************************
-- **************************************************
-- **************************************************
-- **************************************************
-- **************************************************


-- ************************************************************************
-- return back "bad" tables
-- ************************************************************************

PRINT '--'
PRINT '--Restore bad tables section start time: ' + cast(getdate() as varchar(40))
PRINT '--'


PRINT '-- ***'
PRINT '-- RESTORING BAD TABLES'

IF NOT EXISTS(SELECT TOP 1 1 FROM tempdb.dbo.[bad_tables])
	GOTO SKIP_RESTORE_TABLES

DECLARE ms_comp_col_cursor CURSOR FOR
SELECT [object_id], table_name, owner, file_group
FROM tempdb.dbo.[bad_tables]

open ms_comp_col_cursor
fetch next from ms_comp_col_cursor into @obj_id, @table_name, @owner, @obj_name

WHILE @@fetch_status >= 0
BEGIN

	PRINT '-- RESTORING TABLE ' + @owner + '.' + @table_name + '....'

	-- prepare filed list
	SET @field_list = ''

	SELECT @field_list = '[' + [col_name] + '],' + @field_list
	FROM tempdb.dbo.bad_table_cols
	WHERE [object_id] = @obj_id --1781581385
		and is_computed = 0
	ORDER BY column_id DESC -- for ordered scan

	SET @field_list = left(@field_list, len(@field_list)-1)

	-- print @field_list

	-- create table statement ....

	SET @stmt = 'CREATE TABLE [' + @owner + '].[' + @table_name + '] (' 

	DECLARE ms_bad_comp_col_cursor CURSOR FOR
	SELECT [col_name], length, [precision], scale, type_name, nullable, is_user_defined, is_identity, is_computed
		, system_type_id, seed_value, increment_value, [definition]
	FROM tempdb.dbo.[bad_table_cols]
	WHERE [object_id] = @obj_id
	ORDER BY column_id ASC

	open ms_bad_comp_col_cursor
	fetch next from ms_bad_comp_col_cursor 
	into @col_name, @length, @precision, @scale, @type_name, @nullable, @is_user_defined, @is_identity
		, @is_computed, @system_type_id, @seed, @increment, @definition

	WHILE @@fetch_status >= 0
	BEGIN
		
		SET @field = CHAR(10) /*+ CHAR(13) */ + @col_name + ' '
		SET @field = @field + 
			case 
			when @is_computed = 1 then 'AS ' + @definition COLLATE $(desired_collation)
			when @is_user_defined = 1 then '[' + @type_name COLLATE $(desired_collation) + ']'
			when @is_identity = 1 then '[' + @type_name COLLATE $(desired_collation) + '] IDENTITY (' 
				+ cast(@seed as varchar(10)) COLLATE $(desired_collation) + ','
				+ cast(@increment as varchar(10)) COLLATE $(desired_collation) + ')'
			-- text, ntext, image, sysname
			when @system_type_id in (34, 35, 99, 256) then '[' + @type_name COLLATE $(desired_collation) + ']'
			-- bigint, bit, datetime, int, smalldatetime, smallint, sql_variant, timestamp, tinyint, real
			--	, uniqueidentifier, money, smallmoney
			when @system_type_id in (127, 104, 61, 56, 58, 52, 98, 189, 48, 36, 60, 122, 59) 
				then '[' + @type_name COLLATE $(desired_collation) + ']'
			-- binary & varbinary
			when @system_type_id in (173, 165) then '[' + @type_name COLLATE $(desired_collation) 
				+ '] (' + cast(@length as varchar(10)) COLLATE $(desired_collation) + ')'
			-- char, nchar, nvarchar, varchar
			when @system_type_id in (175, 239, 231, 167) then '[' + @type_name COLLATE $(desired_collation) 
				+ '] (' + cast(@length as varchar(10)) COLLATE $(desired_collation) + ')'
			-- float 
			when @system_type_id in (62) then '[' + @type_name COLLATE $(desired_collation) + '] (' 
				+ cast(@precision as varchar(10)) COLLATE $(desired_collation) + ')'
			-- decimal & numeric
			when @system_type_id in (106, 108) then '[' + @type_name COLLATE $(desired_collation) + '] (' 
				+ cast(@precision as varchar(10)) COLLATE $(desired_collation)
				+ ',' + cast(@scale as varchar(10)) COLLATE $(desired_collation) + ')'
			end

		IF @is_computed <> 1
			SET @field = @field COLLATE $(desired_collation) + ' ' + @nullable COLLATE $(desired_collation)

		SET @field = @field COLLATE $(desired_collation) + ',' COLLATE $(desired_collation) 

		--PRINT @field

		SET @stmt = @stmt COLLATE $(desired_collation) + @field COLLATE $(desired_collation) 

		fetch next from ms_bad_comp_col_cursor 
		into @col_name, @length, @precision, @scale, @type_name, @nullable, @is_user_defined, @is_identity
			, @is_computed, @system_type_id, @seed, @increment, @definition
	END

	-- close cursor
	CLOSE ms_bad_comp_col_cursor
	DEALLOCATE ms_bad_comp_col_cursor

	-- create table section finished

	SET @stmt = left(@stmt, len(@stmt)-1) -- minus comma

	SET @stmt = @stmt + ') ON [' + @obj_name + ']'
	PRINT '---'
	PRINT @stmt

	IF $(script_only) = 0
		EXEC(@stmt)

	IF @@error <> 0 
	BEGIN
		PRINT '-- FAILED TO CREATE TABLE TABLE FOR COMPUTED COLUMN FAILED. SEE ERROR LOG FOR DETAILS.'
		RAISERROR('FAILED TO CREATE TABLE FOR COMPUTED COLUMN FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
		RETURN
	END

	PRINT '---'	

	-- is identity column here?
	IF EXISTS(	SELECT 1 FROM tempdb.dbo.bad_table_cols
				WHERE [object_id] = @obj_id --1781581385
					and is_identity = 1 )
		SET @flag = 1
	ELSE
		SET @flag = 0

	SET @stmt = ''
	IF @flag = 1
		BEGIN
			SET @stmt = 'SET IDENTITY_INSERT [' + @owner COLLATE $(desired_collation) + '].[' 
				+ @table_name COLLATE $(desired_collation) + '] ON ' + CHAR(10)
			-- PRINT @stmt
			-- IF $(script_only) = 0
			-- EXEC(@stmt)
		END

	-- insert into
	SET @stmt = @stmt + 'INSERT INTO [' + @owner COLLATE $(desired_collation) + '].[' 
				+ @table_name COLLATE $(desired_collation) + '] (' 
				+ @field_list COLLATE $(desired_collation) + ') SELECT ' + @field_list COLLATE $(desired_collation) 
				+ ' FROM tempdb.[' + @owner COLLATE $(desired_collation) + '].[a1234___' 
				+ @table_name COLLATE $(desired_collation) + ']'

	IF @flag = 1
		BEGIN
			SET @stmt = @stmt + CHAR(10) + 'SET IDENTITY_INSERT [' + @owner COLLATE $(desired_collation) + '].[' 
					+ @table_name COLLATE $(desired_collation) + '] OFF'
			-- PRINT @stmt
			-- IF $(script_only) = 0
			--	  EXEC(@stmt)
		END

	PRINT @stmt

	IF $(script_only) = 0
		EXEC(@stmt)

	IF @@error <> 0 
	BEGIN
		PRINT '-- RESTORING TABLE FOR COMPUTED COLUMN FAILED. SEE ERROR LOG FOR DETAILS.'
		RAISERROR('RESTORING TABLE FOR COMPUTED COLUMN FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
		RETURN
	END

	-- drop shadow table
	SET @stmt = 'DROP TABLE tempdb.[' + @owner COLLATE $(desired_collation) + '].[a1234___' 
				+ @table_name COLLATE $(desired_collation) + ']'
	PRINT @stmt

	IF $(script_only) = 0
		EXEC(@stmt)

	IF @@error <> 0 
	BEGIN
		PRINT '-- FAILED TO DROP SHADOW TABLE FOR COMPUTED COLUMN. SEE ERROR LOG FOR DETAILS.'
		-- that is not a reason to stop processing
		--RAISERROR('FAILED TO DROP SHADOW TABLE FOR COMPUTED COLUMN. SEE ERROR LOG FOR DETAILS.', 16, 1)
		--RETURN
	END

	fetch next from ms_comp_col_cursor into @obj_id, @table_name, @owner, @obj_name
END

-- close cursor
CLOSE ms_comp_col_cursor
DEALLOCATE ms_comp_col_cursor

SKIP_RESTORE_TABLES:

PRINT '-- RESTORING TABLES FOR COMPUTED COLUMNS passed'

PRINT '--'
PRINT '--Restore bad tables section end time: ' + cast(getdate() as varchar(40))
PRINT '--'

-- ************************************************************************
-- recreate computed columns
-- ************************************************************************
PRINT '-- ***'
PRINT '-- RE-CREATE COMPUTED COLUMNS'

IF NOT EXISTS(SELECT TOP 1 1 FROM tempdb.dbo.[comp_cols] WHERE drop_safe = 0)
	GOTO SKIP_CREATE_COMP_COLS

-- order by is important to create computed columns in proper order
DECLARE ms_comp_col_cursor2 CURSOR FOR
SELECT table_name, owner, [col_name], definition FROM tempdb.dbo.[comp_cols]
WHERE drop_safe = 0
order by owner, table_name, column_id

open ms_comp_col_cursor2
fetch next from ms_comp_col_cursor2 into @table_name, @owner, @col_name, @obj_def

WHILE @@fetch_status >= 0
BEGIN
	SET @stmt = 'ALTER TABLE [' + @owner + '].[' + @table_name + '] ADD [' + @col_name + '] AS ' + @obj_def
	PRINT @stmt

	IF $(script_only) = 0
		EXEC(@stmt)

	IF @@error <> 0 
	BEGIN
		PRINT '-- CREATE COMPUTED COLUMN FAILED. SEE ERROR LOG FOR DETAILS.'
		RAISERROR('CREATE COMPUTED COLUMN FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
		RETURN
	END

	fetch next from ms_comp_col_cursor2 into @table_name, @owner, @col_name, @obj_def
END

-- close cursor
CLOSE ms_comp_col_cursor2
DEALLOCATE ms_comp_col_cursor2

SKIP_CREATE_COMP_COLS:

PRINT '-- RE-CREATE COMPUTED COLUMNS passed'

-- ************************************************************************
-- recreate primary key constraints
-- ************************************************************************
PRINT '-- ***'
PRINT '-- CREATE PRIMARY KEY CONSTRAINTS'

IF NOT EXISTS(SELECT TOP 1 1 FROM tempdb.dbo.[pk_constr_cols])
	GOTO SKIP_CREATE_PK

DECLARE ms_pk_cursor2 CURSOR FOR
SELECT 	[table_name], [owner], [idx_name], [idx_type], [idx_type_desc], [is_unique], [ignore_dup_key],
	[is_unique_constraint], [fill_factor], [is_padded], [is_disabled], [allow_row_locks], [allow_page_locks],
	[col_name], [key_ordinal], [is_descending_key], [is_included_column], [fg_name]
FROM tempdb.dbo.[pk_constr_cols]
order by [owner], [table_name], [idx_name], key_ordinal

open ms_pk_cursor2
fetch next from ms_pk_cursor2 into @table_name, @owner, @idx_name, @idx_type, @idx_type_desc, @is_unique, @ignore_dup_key,
	@is_unique_constraint, @fill_factor, @is_padded, @is_disabled, @allow_row_locks, @allow_page_locks,
	@col_name, @key_ordinal, @is_descending_key, @is_included_column, @fg_name

SELECT @col_list1 = '[' + @col_name + ']' + case @is_descending_key when 0 then ' ASC' else ' DESC' end

WHILE @@fetch_status >= 0
BEGIN

	fetch next from ms_pk_cursor2 into @table_name1, @owner1, @idx_name1, @idx_type1, @idx_type_desc1, @is_unique1
		, @ignore_dup_key1, @is_unique_constraint1, @fill_factor1, @is_padded1, @is_disabled1, @allow_row_locks1
		, @allow_page_locks1, @col_name1, @key_ordinal1, @is_descending_key1, @is_included_column1, @fg_name1

	IF @@fetch_status < 0 BREAK

	IF @table_name = @table_name1 AND @owner = @owner1 AND @idx_name = @idx_name1
	  BEGIN
			SET @col_list1 = @col_list1 COLLATE $(desired_collation) 
				+ ',[' + @col_name1 COLLATE $(desired_collation) + ']'
				+ case @is_descending_key when 0 then ' ASC' else ' DESC' end COLLATE $(desired_collation) 
	  END
	ELSE
	  BEGIN

		-- ALTER TABLE [dbo].[Orders] ADD  CONSTRAINT [PK_Orders] PRIMARY KEY CLUSTERED 
		--([OrderID] ASC) WITH (PAD_INDEX  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, 
		--ONLINE = OFF) ON [PRIMARY]		
		SET @stmt = 'ALTER TABLE [' + @owner + '].[' + @table_name + '] ADD CONSTRAINT [' + @idx_name 
			+ '] PRIMARY KEY ' + @idx_type_desc + ' (' + @col_list1 + ')' 

		SET @stmt = @stmt + ' WITH ('
		SET @stmt = @stmt + 'PAD_INDEX=' + case @is_padded when 0 then 'OFF' ELSE 'ON' END
		SET @stmt = @stmt + ', IGNORE_DUP_KEY=' + case @ignore_dup_key when 0 then 'OFF' ELSE 'ON' END
		SET @stmt = @stmt + ', ALLOW_ROW_LOCKS=' + case @allow_row_locks when 0 then 'OFF' ELSE 'ON' END
		SET @stmt = @stmt + ', ALLOW_PAGE_LOCKS=' + case @allow_page_locks when 0 then 'OFF' ELSE 'ON' END

		IF @fill_factor <> 0 
			SET @stmt = @stmt + ', FILLFACTOR=' + cast(@fill_factor as varchar(3))

		SET @stmt = @stmt + ') ON [' + @fg_name + ']'

		PRINT @stmt
		IF $(script_only) = 0
			EXEC(@stmt)

		IF @@error <> 0 
		BEGIN
			PRINT '-- CREATE PK CONSTRAINT FAILED. SEE ERROR LOG FOR DETAILS.'
			RAISERROR('CREATE PK CONSTRAINT FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
			RETURN
		END

		IF @is_disabled = 1
		BEGIN
			SET @stmt = 'ALTER TABLE [' + @owner + '].[' + @table_name + '] NOCHECK CONSTRAINT [' + @idx_name + ']'
			PRINT @stmt
			IF $(script_only) = 0
				EXEC(@stmt)

			IF @@error <> 0 
			BEGIN
				PRINT '-- DISABLE PK CONSTRAINT FAILED. SEE ERROR LOG FOR DETAILS.'
				RAISERROR('DISABLE PK CONSTRAINT FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
				RETURN
			END

		END

		SELECT @table_name = @table_name1, @owner = @owner1, @idx_name = @idx_name1, @idx_type = @idx_type1
			, @idx_type_desc = @idx_type_desc1, @is_unique = @is_unique1, @ignore_dup_key = @ignore_dup_key1
			, @is_unique_constraint = @is_unique_constraint1, @fill_factor = @fill_factor1, @is_padded = @is_padded1
			, @is_disabled = @is_disabled1, @allow_row_locks = @allow_row_locks1, @allow_page_locks = @allow_page_locks1
			, @col_name = @col_name1, @key_ordinal = @key_ordinal1, @is_descending_key = @is_descending_key1
			, @is_included_column = @is_included_column1, @fg_name = @fg_name1

		SELECT @col_list1 = '[' + @col_name + ']' + case @is_descending_key when 0 then ' ASC' else ' DESC' end

	  END --@table_name = @table_name1 AND @owner = @owner1 AND @idx_name = @idx_name1

END

-- close cursor
CLOSE ms_pk_cursor2
DEALLOCATE ms_pk_cursor2

-- ALTER TABLE [dbo].[Orders] ADD  CONSTRAINT [PK_Orders] PRIMARY KEY CLUSTERED 
--([OrderID] ASC) WITH (PAD_INDEX  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, 
--ONLINE = OFF) ON [PRIMARY]		
SET @stmt = 'ALTER TABLE [' + @owner + '].[' + @table_name + '] ADD CONSTRAINT [' + @idx_name 
	+ '] PRIMARY KEY ' + @idx_type_desc + ' (' + @col_list1 + ')' 

SET @stmt = @stmt + ' WITH ('
SET @stmt = @stmt + 'PAD_INDEX=' + case @is_padded when 0 then 'OFF' ELSE 'ON' END
SET @stmt = @stmt + ', IGNORE_DUP_KEY=' + case @ignore_dup_key when 0 then 'OFF' ELSE 'ON' END
SET @stmt = @stmt + ', ALLOW_ROW_LOCKS=' + case @allow_row_locks when 0 then 'OFF' ELSE 'ON' END
SET @stmt = @stmt + ', ALLOW_PAGE_LOCKS=' + case @allow_page_locks when 0 then 'OFF' ELSE 'ON' END

IF @fill_factor <> 0 
	SET @stmt = @stmt + ', FILLFACTOR=' + cast(@fill_factor as varchar(3))

SET @stmt = @stmt + ') ON [' + @fg_name + ']'

PRINT @stmt

IF $(script_only) = 0
	EXEC(@stmt)

IF @@error <> 0 
BEGIN
	PRINT '-- CREATE PK CONSTRAINT FAILED. SEE ERROR LOG FOR DETAILS.'
	RAISERROR('CREATE PK CONSTRAINT FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
	RETURN
END

IF @is_disabled = 1
BEGIN
	SET @stmt = 'ALTER TABLE [' + @owner + '].[' + @table_name + '] NOCHECK CONSTRAINT [' + @idx_name + ']'
	PRINT @stmt

	IF $(script_only) = 0
		EXEC(@stmt)

	IF @@error <> 0 
	BEGIN
		PRINT '-- DISABLE PK CONSTRAINT FAILED. SEE ERROR LOG FOR DETAILS.'
		RAISERROR('DISABLE PK CONSTRAINT FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
		RETURN
	END

END

SKIP_CREATE_PK:

PRINT '-- CREATE PRIMARY KEY CONSTRAINTS passed'

-- *************************************************
-- recreate default constraints
-- *************************************************
PRINT '-- ***'
PRINT '-- CREATE DEFAULT CONSTRAINTS'

IF NOT EXISTS(SELECT TOP 1 1 FROM tempdb.dbo.[def_constr])
	GOTO SKIP_CREATE_DEFAULTS

DECLARE ms_dc_cursor2 CURSOR FOR
SELECT [table_name], [owner], default_name, [col_name], definition FROM tempdb.[dbo].[def_constr]

open ms_dc_cursor2
fetch next from ms_dc_cursor2 into @table_name, @owner, @obj_name, @col_name, @obj_def

WHILE @@fetch_status >= 0
BEGIN
	SET @stmt = 'ALTER TABLE [' + @owner + '].[' + @table_name + '] ADD CONSTRAINT [' + 
			@obj_name + '] DEFAULT ' + @obj_def + ' FOR ['  + @col_name + ']'
	PRINT @stmt

	IF $(script_only) = 0
		EXEC(@stmt)

	IF @@error <> 0 
	BEGIN
		PRINT '-- CREATE DEFAULT CONSTRAINT FAILED. SEE ERROR LOG FOR DETAILS.'
		RAISERROR('CREATE DEFAULT CONSTRAINT FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
		RETURN
	END

	fetch next from ms_dc_cursor2 into @table_name, @owner, @obj_name, @col_name, @obj_def
END

-- close cursor
CLOSE ms_dc_cursor2
DEALLOCATE ms_dc_cursor2

SKIP_CREATE_DEFAULTS:

PRINT '-- CREATE DEFAULT CONSTRAINTS passed'

-- ************************************************************************
-- recreate check constraints
-- ************************************************************************
PRINT '-- ***'
PRINT '-- CREATE CHECK CONSTRAINTS'

IF NOT EXISTS(SELECT TOP 1 1 FROM tempdb.dbo.[check_constr])
	GOTO SKIP_CREATE_CHECK_CONSTRAINTS

DECLARE ms_cc_cursor2 CURSOR FOR
SELECT [name], [owner], par_obj_name, definition, with_check FROM tempdb.dbo.[check_constr]

open ms_cc_cursor2
fetch next from ms_cc_cursor2 into @obj_name, @owner, @table_name, @obj_def, @with_check

WHILE @@fetch_status >= 0
BEGIN

	SET @stmt = 'ALTER TABLE [' + @owner + '].[' + @table_name + '] ' + @with_check + ' ADD CONSTRAINT [' + 
			@obj_name + '] CHECK ' + @obj_def
	PRINT @stmt

	IF $(script_only) = 0
		EXEC(@stmt)

	IF @@error <> 0 
	BEGIN
		PRINT '-- CREATE CHECK CONSTRAINT FAILED. SEE ERROR LOG FOR DETAILS.'
		RAISERROR('CREATE CHECK CONSTRAINT FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
		RETURN
	END

	fetch next from ms_cc_cursor2 into @obj_name, @owner, @table_name, @obj_def, @with_check
END

-- close cursor
CLOSE ms_cc_cursor2
DEALLOCATE ms_cc_cursor2

SKIP_CREATE_CHECK_CONSTRAINTS:

PRINT '-- CREATE CHECK CONSTRAINTS passed'

-- ****************************************************************
-- recreate indexes
-- ****************************************************************
PRINT '-- ***'
PRINT '-- CREATE INDEXES'

IF NOT EXISTS(SELECT TOP 1 1 FROM tempdb.dbo.[ind_columns])
	GOTO SKIP_CREATE_INDEXES

DECLARE ms_ind_cursor2 CURSOR FOR
SELECT 	[table_name], [owner], [idx_name], [idx_type], [idx_type_desc], [is_unique], [ignore_dup_key],
	[is_unique_constraint], [fill_factor], [is_padded], [is_disabled], [allow_row_locks], [allow_page_locks],
	[col_name], [key_ordinal], [is_descending_key], [is_included_column], [fg_name]
FROM tempdb.dbo.[ind_columns]
order by [owner], [table_name], [idx_name], key_ordinal

open ms_ind_cursor2
fetch next from ms_ind_cursor2 into @table_name, @owner, @idx_name, @idx_type, @idx_type_desc, @is_unique, @ignore_dup_key,
	@is_unique_constraint, @fill_factor, @is_padded, @is_disabled, @allow_row_locks, @allow_page_locks,
	@col_name, @key_ordinal, @is_descending_key, @is_included_column, @fg_name

SELECT @col_list1 = '[' + @col_name + ']' + case @is_descending_key when 0 then ' ASC' else ' DESC' end

WHILE @@fetch_status >= 0
BEGIN

	fetch next from ms_ind_cursor2 into @table_name1, @owner1, @idx_name1, @idx_type1, @idx_type_desc1, @is_unique1
		, @ignore_dup_key1, @is_unique_constraint1, @fill_factor1, @is_padded1, @is_disabled1, @allow_row_locks1
		, @allow_page_locks1, @col_name1, @key_ordinal1, @is_descending_key1, @is_included_column1, @fg_name1

	IF @@fetch_status < 0 BREAK

	IF @table_name = @table_name1 AND @owner = @owner1 AND @idx_name = @idx_name1
	  BEGIN
			SET @col_list1 = @col_list1 COLLATE $(desired_collation) + ',[' 
				+ @col_name1 COLLATE $(desired_collation) + ']'
				+ case @is_descending_key when 0 then ' ASC' else ' DESC' end COLLATE $(desired_collation) 
	  END
	ELSE
	  BEGIN

		IF @is_unique_constraint = 1
			BEGIN
				-- ALTER TABLE [dbo].[ccc] ADD CONSTRAINT [ccc_id2_unique] UNIQUE NONCLUSTERED 
				--( [id2] ASC ) WITH (PAD_INDEX  = OFF, IGNORE_DUP_KEY = OFF)

				--SET @stmt = 'ALTER TABLE [' + @owner + '].[' + @table_name + '] ADD CONSTRAINT [' + @idx_name 
				--	+ '] ' + case when @is_unique = 1 then 'UNIQUE ' else ' ' end + @idx_type_desc + ' (' + @col_list1 + ')' 

				SET @stmt = 'ALTER TABLE [' + @owner + '].[' + @table_name + '] ADD CONSTRAINT [' + @idx_name 
					+ '] UNIQUE ' + @idx_type_desc + ' (' + @col_list1 + ')' 

				SET @stmt = @stmt + ' WITH ('

				SET @stmt = @stmt + 'PAD_INDEX=' + case @is_padded when 0 then 'OFF' ELSE 'ON' END
				SET @stmt = @stmt + ', IGNORE_DUP_KEY=' + case @ignore_dup_key when 0 then 'OFF' ELSE 'ON' END
				SET @stmt = @stmt + ', ALLOW_ROW_LOCKS=' + case @allow_row_locks when 0 then 'OFF' ELSE 'ON' END
				SET @stmt = @stmt + ', ALLOW_PAGE_LOCKS=' + case @allow_page_locks when 0 then 'OFF' ELSE 'ON' END

				IF @fill_factor <> 0 
					SET @stmt = @stmt + ', FILLFACTOR=' + cast(@fill_factor as varchar(3))

				SET @stmt = @stmt + ') ON [' + @fg_name + ']'

				PRINT @stmt

				IF $(script_only) = 0
					EXEC(@stmt)

				IF @@error <> 0 
				BEGIN
					PRINT '-- CREATE UNIQUE CONSTRAINT FAILED. SEE ERROR LOG FOR DETAILS.'
					RAISERROR('CREATE UNIQUE CONSTRAINT FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
					RETURN
				END

				IF @is_disabled = 1
				BEGIN
					SET @stmt = 'ALTER TABLE [' + @owner + '].[' + @table_name + '] NOCHECK CONSTRAINT [' + @idx_name + ']'
					PRINT @stmt

					IF $(script_only) = 0
						EXEC(@stmt)

					IF @@error <> 0 
					BEGIN
						PRINT '-- DISABLE UNIQUE CONSTRAINT FAILED. SEE ERROR LOG FOR DETAILS.'
						RAISERROR('DISABLE UNIQUE CONSTRAINT FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
						RETURN
					END

				END

			END
		ELSE
			BEGIN
				-- CREATE UNIQUE NONCLUSTERED INDEX [IX_bbb_two] ON [dbo].[bbb] 
				-- ([bbb_id] ASC, [bbb_text] ASC) 
				-- WITH (PAD_INDEX  = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF) ON [PRIMARY]

				SET @stmt = 'CREATE ' +
					CASE @is_unique WHEN 1 THEN 'UNIQUE ' ELSE ' ' END + @idx_type_desc + 
					' INDEX [' + @idx_name + '] ON [' + @owner + '].[' + @table_name + '] (' + @col_list1 + ')' 

				SET @stmt = @stmt + ' WITH ('

				SET @stmt = @stmt + 'PAD_INDEX=' + case @is_padded when 0 then 'OFF' ELSE 'ON' END
				SET @stmt = @stmt + ', IGNORE_DUP_KEY=' + case @ignore_dup_key when 0 then 'OFF' ELSE 'ON' END
				SET @stmt = @stmt + ', ALLOW_ROW_LOCKS=' + case @allow_row_locks when 0 then 'OFF' ELSE 'ON' END
				SET @stmt = @stmt + ', ALLOW_PAGE_LOCKS=' + case @allow_page_locks when 0 then 'OFF' ELSE 'ON' END

				IF @fill_factor <> 0 
					SET @stmt = @stmt + ', FILLFACTOR=' + cast(@fill_factor as varchar(3))

				SET @stmt = @stmt + ') ON [' + @fg_name + ']'

				PRINT @stmt

				IF $(script_only) = 0
					EXEC(@stmt)

				IF @@error <> 0 
				BEGIN
					PRINT '-- CREATE INDEX FAILED. SEE ERROR LOG FOR DETAILS.'
					RAISERROR('CREATE INDEX FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
					RETURN
				END

				IF @is_disabled = 1
				BEGIN
					SET @stmt = 'ALTER INDEX [' + @idx_name + ']' + ' ON [' + @owner + '].[' + @table_name + '] DISABLE'
					PRINT @stmt

					IF $(script_only) = 0
						EXEC(@stmt)

					IF @@error <> 0 
					BEGIN
						PRINT '-- DISABLE INDEX FAILED. SEE ERROR LOG FOR DETAILS.'
						RAISERROR('DISABLE INDEX FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
						RETURN
					END

				END

			END -- @is_unique_constraint = 1


		SELECT @table_name = @table_name1, @owner = @owner1, @idx_name = @idx_name1, @idx_type = @idx_type1
			, @idx_type_desc = @idx_type_desc1, @is_unique = @is_unique1, @ignore_dup_key = @ignore_dup_key1
			, @is_unique_constraint = @is_unique_constraint1, @fill_factor = @fill_factor1, @is_padded = @is_padded1
			, @is_disabled = @is_disabled1, @allow_row_locks = @allow_row_locks1, @allow_page_locks = @allow_page_locks1
			, @col_name = @col_name1, @key_ordinal = @key_ordinal1, @is_descending_key = @is_descending_key1
			, @is_included_column = @is_included_column1, @fg_name = @fg_name1

		SELECT @col_list1 = '[' + @col_name + ']' + case @is_descending_key when 0 then ' ASC' else ' DESC' end

	  END --@table_name = @table_name1 AND @owner = @owner1 AND @idx_name = @idx_name1

END

-- close cursor
CLOSE ms_ind_cursor2
DEALLOCATE ms_ind_cursor2

IF @is_unique_constraint = 1
	BEGIN
		-- ALTER TABLE [dbo].[ccc] ADD CONSTRAINT [ccc_id2_unique] UNIQUE NONCLUSTERED 
		--( [id2] ASC ) WITH (PAD_INDEX  = OFF, IGNORE_DUP_KEY = OFF)
		SET @stmt = 'ALTER TABLE [' + @owner + '].[' + @table_name + '] ADD CONSTRAINT [' + @idx_name 
			+ '] UNIQUE ' + @idx_type_desc + ' (' + @col_list1 + ')' 

		SET @stmt = @stmt + ' WITH ('

		SET @stmt = @stmt + 'PAD_INDEX=' + case @is_padded when 0 then 'OFF' ELSE 'ON' END
		SET @stmt = @stmt + ', IGNORE_DUP_KEY=' + case @ignore_dup_key when 0 then 'OFF' ELSE 'ON' END
		SET @stmt = @stmt + ', ALLOW_ROW_LOCKS=' + case @allow_row_locks when 0 then 'OFF' ELSE 'ON' END
		SET @stmt = @stmt + ', ALLOW_PAGE_LOCKS=' + case @allow_page_locks when 0 then 'OFF' ELSE 'ON' END

		IF @fill_factor <> 0 
			SET @stmt = @stmt + ', FILLFACTOR=' + cast(@fill_factor as varchar(3))

		SET @stmt = @stmt + ') ON [' + @fg_name + ']'

		PRINT @stmt

		IF $(script_only) = 0
			EXEC(@stmt)

		IF @@error <> 0 
		BEGIN
			PRINT '-- CREATE UNIQUE CONSTRAINT FAILED. SEE ERROR LOG FOR DETAILS.'
			RAISERROR('CREATE UNIQUE CONSTRAINT FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
			RETURN
		END

		IF @is_disabled = 1
		BEGIN
			SET @stmt = 'ALTER TABLE [' + @owner + '].[' + @table_name + '] NOCHECK CONSTRAINT [' + @idx_name + ']'
			PRINT @stmt

			IF $(script_only) = 0
				EXEC(@stmt)

			IF @@error <> 0 
			BEGIN
				PRINT '-- DISABLE UNIQUE CONSTRAINT FAILED. SEE ERROR LOG FOR DETAILS.'
				RAISERROR('DISABLE UNIQUE CONSTRAINT FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
				RETURN
			END
		END

	END
ELSE
	BEGIN
		-- CREATE UNIQUE NONCLUSTERED INDEX [IX_bbb_two] ON [dbo].[bbb] 
		-- ([bbb_id] ASC, [bbb_text] ASC) 
		-- WITH (PAD_INDEX  = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF) ON [PRIMARY]

		SET @stmt = 'CREATE ' +
			CASE @is_unique WHEN 1 THEN 'UNIQUE ' ELSE ' ' END + @idx_type_desc + 
			' INDEX [' + @idx_name + '] ON [' + @owner + '].[' + @table_name + '] (' + @col_list1 + ')' 

		SET @stmt = @stmt + ' WITH ('

		SET @stmt = @stmt + 'PAD_INDEX=' + case @is_padded when 0 then 'OFF' ELSE 'ON' END
		SET @stmt = @stmt + ', IGNORE_DUP_KEY=' + case @ignore_dup_key when 0 then 'OFF' ELSE 'ON' END
		SET @stmt = @stmt + ', ALLOW_ROW_LOCKS=' + case @allow_row_locks when 0 then 'OFF' ELSE 'ON' END
		SET @stmt = @stmt + ', ALLOW_PAGE_LOCKS=' + case @allow_page_locks when 0 then 'OFF' ELSE 'ON' END

		IF @fill_factor <> 0 
			SET @stmt = @stmt + ', FILLFACTOR=' + cast(@fill_factor as varchar(3))

		SET @stmt = @stmt + ') ON [' + @fg_name + ']'

		PRINT @stmt

		IF $(script_only) = 0
			EXEC(@stmt)

		IF @@error <> 0 
		BEGIN
			PRINT '-- CREATE INDEX FAILED. SEE ERROR LOG FOR DETAILS.'
			RAISERROR('CREATE INDEX FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
			RETURN
		END

		IF @is_disabled = 1
		BEGIN
			SET @stmt = 'ALTER INDEX [' + @idx_name + ']' + ' ON [' + @owner + '].[' + @table_name + '] DISABLE'
			PRINT @stmt

			IF $(script_only) = 0
				EXEC(@stmt)

			IF @@error <> 0 
			BEGIN
				PRINT '-- DISABLE INDEX FAILED. SEE ERROR LOG FOR DETAILS.'
				RAISERROR('DISABLE INDEX FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
				RETURN
			END

		END
	END

SKIP_CREATE_INDEXES:

PRINT '-- CREATE INDEXES passed'


-- ************************************************************************
-- recreate fk constraints constraints
-- ************************************************************************
PRINT '-- ***'
PRINT '-- CREATE FK CONSTRAINTS'

IF NOT EXISTS(SELECT TOP 1 1 FROM tempdb.dbo.fk_constr_cols)
	GOTO SKIP_CREATE_FK

DECLARE ms_fk_cursor2 CURSOR FOR
select foreign_key_name, table_name, owner, constraint_column_name, referenced_object, ref_owner, referenced_column_name
	, is_disabled, is_not_for_replication, delete_referential_action, update_referential_action, with_check
from tempdb.dbo.fk_constr_cols
order by foreign_key_name, constraint_column_id

open ms_fk_cursor2
fetch next from ms_fk_cursor2 into @fk_name, @table_name, @owner, @constraint_column_name 
	, @referenced_object, @ref_owner, @referenced_column_name, @is_disabled
	, @is_not_for_replication, @delete_referential_action, @update_referential_action, @with_check

SELECT @col_list1 = '[' + @constraint_column_name + ']'
	, @col_list2 = '[' + @referenced_column_name + ']'

WHILE @@fetch_status >= 0
BEGIN

	fetch next from ms_fk_cursor2 into @fk_name1, @table_name1, @owner1, @constraint_column_name1 
	, @referenced_object1, @ref_owner1, @referenced_column_name1, @is_disabled1
	, @is_not_for_replication1, @delete_referential_action1, @update_referential_action1, @with_check1

	IF @@fetch_status < 0 BREAK

	IF @fk_name <> @fk_name1
	  BEGIN
		-- new row, add constraint for prev row

		--ALTER TABLE [dbo].[bbb]  WITH CHECK ADD  CONSTRAINT [FK_bbb_bb] FOREIGN KEY([bbb_id1], [bbb_id2])
		--REFERENCES [dbo].[bb] ([bb_id1], [bb_id2])

		SET @stmt = 'ALTER TABLE [' + @owner + '].[' + @table_name + '] ' + @with_check + ' ADD CONSTRAINT [' + 
			@fk_name + '] FOREIGN KEY (' + @col_list1 + ') REFERENCES [' + @ref_owner + '].[' + 
			@referenced_object + '] (' + @col_list2 + ')'

		IF @update_referential_action = 1
			SET @stmt = @stmt + ' ON UPDATE CASCADE'

		IF @is_not_for_replication = 1
			SET @stmt = @stmt + ' NOT FOR REPLICATION'

		IF @delete_referential_action = 1
			SET @stmt = @stmt + ' ON DELETE CASCADE'

		PRINT @stmt

		IF $(script_only) = 0
			EXEC(@stmt)

		IF @@error <> 0 
		BEGIN
			PRINT '-- CREATE FK CONSTRAINT FAILED. SEE ERROR LOG FOR DETAILS.'
			RAISERROR('CREATE FK CONSTRAINT FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
			RETURN
		END

		IF @is_disabled = 1
		BEGIN
			SET @stmt = 'ALTER TABLE [' + @owner + '].[' + @table_name + '] NOCHECK CONSTRAINT [' + @fk_name + ']'
			PRINT @stmt

			IF $(script_only) = 0
				EXEC(@stmt)

			IF @@error <> 0 
			BEGIN
				PRINT '-- DISABLE FK CONSTRAINT FAILED. SEE ERROR LOG FOR DETAILS.'
				RAISERROR('DISABLE FK CONSTRAINT FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
				RETURN
			END

		END

		SELECT @fk_name = @fk_name1, @table_name = @table_name1, @owner = @owner1
			, @constraint_column_name = @constraint_column_name1 
			, @referenced_object = @referenced_object1, @ref_owner = @ref_owner1
			, @referenced_column_name = @referenced_column_name1, @is_disabled = @is_disabled1
			, @is_not_for_replication = @is_not_for_replication1
			, @delete_referential_action = @delete_referential_action1
			, @update_referential_action = @update_referential_action1
			, @with_check = @with_check1

		SELECT @col_list1 = '[' + @constraint_column_name + ']'
			, @col_list2 = '[' + @referenced_column_name + ']'

	  END -- @fk_name <> @fk_name1
	ELSE
	  BEGIN
			SELECT @col_list1 = @col_list1 COLLATE $(desired_collation) + ',[' 
				+ @constraint_column_name1 COLLATE $(desired_collation) + ']'
				, @col_list2 = @col_list2 COLLATE $(desired_collation) 
				+ ',[' + @referenced_column_name1 COLLATE $(desired_collation) + ']'
	  END

END

SET @stmt = 'ALTER TABLE [' + @owner + '].[' + @table_name + '] ' + @with_check + ' ADD CONSTRAINT [' + 
	@fk_name + '] FOREIGN KEY (' + @col_list1 + ') REFERENCES [' + @ref_owner + '].[' + 
	@referenced_object + '] (' + @col_list2 + ')'

IF @update_referential_action = 1
	SET @stmt = @stmt + ' ON UPDATE CASCADE'

IF @is_not_for_replication = 1
	SET @stmt = @stmt + ' NOT FOR REPLICATION'

IF @delete_referential_action = 1
	SET @stmt = @stmt + ' ON DELETE CASCADE'

PRINT @stmt

IF $(script_only) = 0
	EXEC(@stmt)

IF @@error <> 0 
BEGIN
	PRINT '-- CREATE FK CONSTRAINT FAILED. SEE ERROR LOG FOR DETAILS.'
	RAISERROR('CREATE FK CONSTRAINT FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
	RETURN
END

IF @is_disabled = 1
BEGIN
	SET @stmt = 'ALTER TABLE [' + @owner + '].[' + @table_name + '] NOCHECK CONSTRAINT [' + @fk_name + ']'
	PRINT @stmt

	IF $(script_only) = 0
		EXEC(@stmt)

	IF @@error <> 0 
	BEGIN
		PRINT '-- DISABLE FK CONSTRAINT FAILED. SEE ERROR LOG FOR DETAILS.'
		RAISERROR('DISABLE FK CONSTRAINT FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
		RETURN
	END

END

-- close cursor
CLOSE ms_fk_cursor2
DEALLOCATE ms_fk_cursor2

SKIP_CREATE_FK:

PRINT '-- CREATE FK CONSTRAINTS passed'

-- ****************************************************************
-- re-create statistics
-- ****************************************************************
PRINT '-- ***'
PRINT '-- RE-CREATE HAND-MADE STATISTICS'

IF NOT EXISTS(SELECT TOP 1 1 FROM tempdb.dbo.[user_stat_cols])
	GOTO SKIP_CREATE_USER_STAT

DECLARE ms_stat_cursor2 CURSOR FOR
SELECT owner, table_name, stat_name, column_name, no_recompute 
FROM tempdb.dbo.[user_stat_cols]
ORDER BY owner, table_name, stat_name, col_order

open ms_stat_cursor2
fetch next from ms_stat_cursor2 into @owner, @table_name, @obj_name, @col_name, @flag

--PRINT @col_name
--PRINT '----fs ' + cast(@@fetch_status as varchar(20))

SET @col_list1 = '[' + @col_name + ']'

WHILE @@fetch_status >= 0
BEGIN

	fetch next from ms_stat_cursor2 into @owner1, @table_name1, @obj_name1, @col_name1, @flag1

	IF @@fetch_status < 0 BREAK

	--PRINT '----fs ' + cast(@@fetch_status as varchar(20))
	--PRINT '---' + @col_name1

	IF NOT (@owner = @owner1 AND @table_name = @table_name1 AND @obj_name = @obj_name1)
	  BEGIN
		-- CREATE STATISTICS [Stat_certificates] ON [dbo].[CERTIFICATES]([Subject], [Issuer]) WITH NORECOMPUTE

		SET @stmt = 'CREATE STATISTICS [' + @obj_name + '] ON [' + @owner + '].[' + @table_name + '] (' +
			@col_list1 + ')'

		IF @flag = 1
			SET @stmt = @stmt + ' WITH NORECOMPUTE'

		PRINT @stmt

		IF $(script_only) = 0
			EXEC(@stmt)

		IF @@error <> 0 
		BEGIN
			PRINT 'RE-CREATE STATISTICS FAILED. SEE ERROR LOG FOR DETAILS.'
			RAISERROR('RE-CREATE STATISTICS FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
			RETURN
		END

		SELECT @owner = @owner1, @table_name = @table_name1, @obj_name = @obj_name1, @col_name = @col_name1
			, @flag = @flag1

		SET @col_list1 = '[' + @col_name COLLATE $(desired_collation) + ']'

	  END 
	ELSE
	  BEGIN
			SET @col_list1 = @col_list1 COLLATE $(desired_collation) + ',[' + @col_name1 COLLATE $(desired_collation) + ']'
	  END

END

-- close cursor
CLOSE ms_stat_cursor2
DEALLOCATE ms_stat_cursor2

SET @stmt = 'CREATE STATISTICS [' + @obj_name + '] ON [' + @owner + '].[' + @table_name + '] (' + @col_list1 + ')'

IF @flag = 1
	SET @stmt = @stmt + ' WITH NORECOMPUTE'

PRINT @stmt

IF $(script_only) = 0
	EXEC(@stmt)

IF @@error <> 0 
BEGIN
	PRINT '-- RE-CREATE STATISTICS FAILED. SEE ERROR LOG FOR DETAILS.'
	RAISERROR('RE-CREATE STATISTICS FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
	RETURN
END

SKIP_CREATE_USER_STAT:

PRINT '-- RE-CREATE HAND-MADE STATISTICS passed'


-- **************************************************
-- re-create triggers
-- **************************************************
PRINT '-- ***'
PRINT '-- RE-CREATE TRIGGERS'

IF NOT EXISTS(SELECT TOP 1 1 FROM tempdb.dbo.[triggers])
	GOTO SKIP_CREATE_TRIGGERS

DECLARE ms_trig_cursor2 CURSOR FOR
SELECT [trigger_name], [table_name], [owner], is_disabled, definition, [ansi_nulls], [quoted_identifier]
FROM tempdb.dbo.[triggers]

-- open cursor
open ms_trig_cursor2
fetch next from ms_trig_cursor2 into @obj_name, @table_name, @owner, @flag, @obj_def, @ansi_nulls
	, @quoted_identifier

WHILE @@fetch_status >= 0
BEGIN

	SET @stmt = 'CREATE TRIGGER ' + @obj_name COLLATE $(desired_collation) 
		+ ' ON [' + @owner COLLATE $(desired_collation) + '].[' + @table_name COLLATE $(desired_collation) + ']'
	PRINT @stmt

	IF $(script_only) = 0
		BEGIN

			IF @ansi_nulls = 0 AND @quoted_identifier = 0
				EXEC dbo.usp_ANSINULLS_OFF_QUOTED_OFF @obj_def
			ELSE
				BEGIN
					IF @ansi_nulls = 0 AND @quoted_identifier = 1
						EXEC dbo.usp_ANSINULLS_OFF_QUOTED_ON @obj_def
					ELSE
						BEGIN
							IF @ansi_nulls = 1 AND @quoted_identifier = 0
								EXEC dbo.usp_ANSINULLS_ON_QUOTED_OFF @obj_def
							ELSE -- ON ON
								EXEC dbo.usp_ANSINULLS_ON_QUOTED_ON @obj_def
						END
				END

		END
 
	ELSE 
		BEGIN
			PRINT @obj_def
		END

	IF @@error <> 0 
		BEGIN
			PRINT '-- ********************************************************************'
			PRINT '-- CREATE TRIGGER FAILED. SEE ERROR LOG FOR DETAILS.'
			PRINT '-- ********************************************************************'
			RAISERROR('CREATE TRIGGER FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
			--RETURN
		END

	IF @flag = 1
		BEGIN
			SET @stmt = 'ALTER TABLE [' + @owner + '].[' + @table_name + '] DISABLE TRIGGER [' + @obj_name + ']'
			PRINT @stmt

			IF $(script_only) = 0
				EXEC(@stmt)

			IF @@error <> 0 
				BEGIN
					PRINT '-- ********************************************************************'
					PRINT '-- DISABLE TRIGGER FAILED. SEE ERROR LOG FOR DETAILS.'
					PRINT '-- ********************************************************************'
					RAISERROR('DISABLE TRIGGER FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
					--RETURN
				END
		END

	fetch next from ms_trig_cursor2 into @obj_name, @table_name, @owner, @flag, @obj_def, @ansi_nulls
		, @quoted_identifier
END

-- close cursor
CLOSE ms_trig_cursor2
DEALLOCATE ms_trig_cursor2

SKIP_CREATE_TRIGGERS:

PRINT '-- ENABLE TRIGGERS passed'

-- **************************************************
-- set trigger order
-- **************************************************
PRINT '-- ***'
PRINT '-- SET TRIGGER ORDER'

IF NOT EXISTS(SELECT TOP 1 1 FROM tempdb.dbo.[trigger_order])
	GOTO SKIP_SET_TRIGGER_ORDER

DECLARE ms_trig_cursor3 CURSOR FOR
SELECT [trigger_name], [owner], type_desc, [action]
FROM tempdb.dbo.[trigger_order]

-- open cursor
open ms_trig_cursor3
fetch next from ms_trig_cursor3 into @obj_name, @owner, @obj_name1, @obj_name2

WHILE @@fetch_status >= 0
BEGIN

	SET @new_name = @owner + '.' + @obj_name

	PRINT '-- SET TRIGGER ORDER ' + @obj_name + ' TO ' + @obj_name2 + ' FOR ' + @obj_name1
	EXEC sp_settriggerorder @triggername= @new_name, @order=@obj_name2, @stmttype = @obj_name1

	IF @@error <> 0 
		BEGIN
			PRINT '-- SET TRIGGER ORDER FAILED. SEE ERROR LOG FOR DETAILS.'
			RAISERROR('SET TRIGGER ORDER FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
			RETURN
		END

	fetch next from ms_trig_cursor3 into @obj_name, @owner, @obj_name1, @obj_name2
END

-- close cursor
CLOSE ms_trig_cursor3
DEALLOCATE ms_trig_cursor3

SKIP_SET_TRIGGER_ORDER:

PRINT '-- SET TRIGGER ORDER passed'


-- **************************************************
-- recreate udf & udv
-- **************************************************
PRINT '-- ***'
PRINT '-- RE-CREATE UDF & UDV'

IF NOT EXISTS(SELECT TOP 1 [object_id] FROM tempdb.dbo.udf
				UNION ALL
				SELECT TOP 1 [object_id] FROM tempdb.dbo.udv )
	GOTO SKIP_CREATE_UDO

UPDATE tempdb.dbo.udf
	SET [dropped] = 0
UPDATE tempdb.dbo.udv
	SET [dropped] = 0

DECLARE @t1 int, @t2 int, @name sysname, @first varchar(max)

SET @num_cycles = 0

TRY_CREATE_AGAIN:

SELECT @has_error = 0

DECLARE ms_func_cursor2 CURSOR FOR
SELECT [name] = [udf_name], [owner], [definition], [object_id], [ansi_nulls], [quoted_identifier]
FROM tempdb.dbo.udf
WHERE [dropped] = 0
UNION ALL
SELECT [name] = [udv_name], [owner], [definition], [object_id], [ansi_nulls], [quoted_identifier]
FROM tempdb.dbo.udv
WHERE [dropped] = 0

open ms_func_cursor2
fetch next from ms_func_cursor2 into @obj_name, @owner, @obj_def, @object_id, @ansi_nulls, @quoted_identifier

WHILE @@fetch_status >= 0
BEGIN

	SELECT @t1 = 0, @t2 = 0

	set @name = '[' + @obj_name + ']'
	--PRINT 'name = ' + @name

	-- look for function name with brackets
	set @t1 = charindex( @name, @obj_def )

	IF @t1 = 0
		BEGIN
			set @name = @obj_name
			--PRINT 'name = ' + @name
		END

	-- look for name without brackets
	SET @t1 = charindex( @name, @obj_def)

	IF @t1 = 0 
		BEGIN
			PRINT '-- Could not find object name!'
			RAISERROR ('Could not find object name!', 16, 1)
			RETURN
		END

	-- look for delimiter '.' after owner
	select @t2 = charindex( '.', @obj_def COLLATE $(desired_collation))

	--PRINT 'T1 = ' + cast(@t1 as varchar(5))
	--PRINT 'T2 = ' + cast(@t2 as varchar(5))

	IF @t2 > @t1
		BEGIN
			-- owner is not specified in comments
			SELECT @stmt = LEFT(@obj_def, @t1-1) + @owner + '.' + SUBSTRING(@obj_def, @t1, LEN(@obj_def))
			--PRINT '-- OWNER not found'
			--PRINT @stmt
		END
	ELSE
		BEGIN
			-- owner should be changed
			SELECT @first = LEFT(@obj_def, @t2-1)
			--PRINT 'First = [' + @first + ']'

			IF RIGHT(@first COLLATE $(desired_collation), 1) = ']'
				SET @flag = 1
			ELSE
				SET @flag = 0

			--PRINT 'flag = ' + cast(@flag as varchar(1))

			WHILE RIGHT(@first COLLATE $(desired_collation), 1) = ' ' OR RIGHT(@first COLLATE $(desired_collation), 1) = ']'
				SET @first = LEFT(@first COLLATE $(desired_collation), len(@first COLLATE $(desired_collation))-1)
			--PRINT 'First = [' + @first + ']'

			-- exclude owner
			WHILE NOT( RIGHT(@first COLLATE $(desired_collation), 1) = ' ' OR RIGHT(@first COLLATE $(desired_collation), 1) = '[' )
				SET @first = LEFT(@first COLLATE $(desired_collation), len(@first COLLATE $(desired_collation))-1)
			--PRINT 'First = [' + @first + ']'

			SELECT @stmt = @first + @owner + (case @flag when 1 then ']' else '' end) + '.' 
				+ SUBSTRING(@obj_def, @t1, LEN(@obj_def))

	END

	PRINT '-- ' + @owner + '.' + @obj_name
	PRINT '--'
	PRINT @stmt

	IF $(script_only) = 0

		BEGIN

			IF @ansi_nulls = 0 AND @quoted_identifier = 0
				EXEC dbo.usp_ANSINULLS_OFF_QUOTED_OFF @stmt
			ELSE
				BEGIN
					IF @ansi_nulls = 0 AND @quoted_identifier = 1
						EXEC dbo.usp_ANSINULLS_OFF_QUOTED_ON @stmt
					ELSE
						BEGIN
							IF @ansi_nulls = 1 AND @quoted_identifier = 0
								EXEC dbo.usp_ANSINULLS_ON_QUOTED_OFF @stmt
							ELSE -- ON ON
								EXEC dbo.usp_ANSINULLS_ON_QUOTED_ON @stmt
						END
				END

		END

	IF @@error <> 0 
		BEGIN
			PRINT '-- FAILED to create function or view ' + @owner + '.' + @obj_name + '. Wait for next cycle.'
			SET @has_error = 1
		END
	ELSE
		BEGIN
			PRINT '--' + @owner + '.' + @obj_name + ' has been created.'

			UPDATE tempdb.dbo.udf
				SET [dropped] = 1
			WHERE [object_id] = @object_id

			IF @@error <> 0 
				BEGIN
					PRINT '--Failed to update temp table'
					RAISERROR('Failed to update temp table. SEE ERROR LOG FOR DETAILS.', 16, 1)
					RETURN
				END

			UPDATE tempdb.dbo.udv
				SET [dropped] = 1
			WHERE [object_id] = @object_id

			IF @@error <> 0 
				BEGIN
					PRINT '--Failed to update temp table'
					RAISERROR('Failed to update temp table. SEE ERROR LOG FOR DETAILS.', 16, 1)
					RETURN
				END

		END

	PRINT '--'

	fetch next from ms_func_cursor2 into @obj_name, @owner, @obj_def, @object_id, @ansi_nulls, @quoted_identifier
END

-- close cursor
CLOSE ms_func_cursor2
DEALLOCATE ms_func_cursor2

IF @has_error = 1
	BEGIN
		SET @num_cycles = @num_cycles + 1
		IF @num_cycles > @max_cycles
			BEGIN
				PRINT '-- maximum count of attempts for creating VIEW or FUNCTIUON is reached. SEE ERROR LOG FOR DETAILS.'
				RAISERROR('maximum count of attempts for creating VIEW or FUNCTIUON is reached. SEE ERROR LOG FOR DETAILS.', 16, 1)
				RETURN
			END
		ELSE
			BEGIN
				PRINT '-- trying to create VIEWS or FUNCTIONS again...(' + cast(@num_cycles as varchar(3)) + ')'
				GOTO TRY_CREATE_AGAIN
			END
	END

SKIP_CREATE_UDO:

PRINT '-- CREATE UDF and UDV passes'

-- *********************************************************************
-- recreate permissions
-- *********************************************************************

--затем сравнить две схемы
--стал здесь
--select * from tempdb.dbo.permissions
PRINT '-- ***'
PRINT '-- RE-CREATE PERMISSIONS'

IF NOT EXISTS(SELECT TOP 1 1 FROM tempdb.dbo.[permissions])
	GOTO SKIP_CREATE_PERMISSIONS

DECLARE ms_perm_cursor CURSOR FOR
SELECT [state], [state_desc], [permission_name], [owner], [obj_name], [principal_name], [col_name] 
FROM tempdb.dbo.[permissions]

-- open cursor
open ms_perm_cursor
fetch next from ms_perm_cursor into @mode, @obj_name, @obj_name2, @owner, @table_name, @obj_name3, @col_name

WHILE @@fetch_status >= 0
BEGIN
	--GRANT REFERENCES on [dbo].[udf_TableValued2] to [public]
	IF @mode COLLATE $(desired_collation) = 'W' -- WITH GRANT OPTION
		SET @stmt = 'GRANT ' + @obj_name2 + ' ON [' + @owner + '].[' + @table_name + ']' 
	ELSE
		SET @stmt = @obj_name + ' ' + @obj_name2 + ' ON [' + @owner + '].[' + @table_name + ']' 

	IF LEN(LTRIM(@col_name)) > 0
		SET @stmt = @stmt + '([' + @col_name + '])'
		
	SET @stmt = @stmt + ' TO ' + @obj_name3
	
	IF @mode COLLATE $(desired_collation) = 'W' -- WITH GRANT OPTION
		SET @stmt = @stmt + ' WITH GRANT OPTION'

	PRINT @stmt

	IF $(script_only) = 0
		EXEC(@stmt)

	IF @@error <> 0 
	BEGIN
		PRINT '-- CREATE PERMISSION FAILED. SEE ERROR LOG FOR DETAILS.'
		-- we don't need to raise error here because:
		--		user can set GRANT ALL permission for a functiuon.
		--		sql engine will translate it into GRANT SELECT, GRANT UPDATE, GRANT DELETE
		--		because of GRANT ALL not exists in SQL 2005
		--		but you will not always be able to recreate GRANT DELETE on user-defined table valued function.
		--		that is the reason.
		-- RAISERROR('CREATE PERMISSION FAILED. SEE ERROR LOG FOR DETAILS.', 16, 1)
		-- RETURN
	END

	fetch next from ms_perm_cursor into @mode, @obj_name, @obj_name2, @owner, @table_name, @obj_name3, @col_name
END

-- close cursor
CLOSE ms_perm_cursor
DEALLOCATE ms_perm_cursor

SKIP_CREATE_PERMISSIONS:

PRINT '-- RE-CREATE PERMISSIONS passed'

PRINT '-- *************************************'
PRINT '-- *************************************'
PRINT '-- *************************************'
PRINT '-- DONE. FINAL STEPS...'

PRINT '--'
PRINT '--Re-create object section finished at: ' + cast(getdate() as varchar(40))
PRINT '--'


-- ********************************************************
-- restore original recovery mode
-- ********************************************************
IF @RecoveryModel <> 3
	BEGIN
		PRINT '-- ******'
		PRINT '-- Waiting database $(destdb) to restore recovery mode...'
		SET @stmt = 'ALTER DATABASE $(destdb) SET RECOVERY ' + (case @RecoveryModel when 1 then 'FULL' else 'BULK_LOGGED' end)
		PRINT @stmt
		IF $(script_only) = 0		
			EXEC (@stmt)
		SET @stmt = '-- Database $(destdb) RECOVERY MODE restored to ' + (case @RecoveryModel when 1 then 'FULL' else 'BULK_LOGGED' end)
		PRINT @stmt
	END

-- *********************************************************************
-- final updates
-- *********************************************************************
PRINT 'USE $(destdb)'
USE $(destdb)

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[usp_ANSINULLS_ON_QUOTED_ON]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_ANSINULLS_ON_QUOTED_ON]

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[usp_ANSINULLS_OFF_QUOTED_ON]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_ANSINULLS_OFF_QUOTED_ON]

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[usp_ANSINULLS_ON_QUOTED_OFF]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_ANSINULLS_ON_QUOTED_OFF]

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[usp_ANSINULLS_OFF_QUOTED_OFF]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_ANSINULLS_OFF_QUOTED_OFF]

--DBCC FREEPROCCACHE
--DBCC DROPCLEANBUFFERS
PRINT 'DBCC UPDATEUSAGE (0)'
IF $(script_only) = 0		
	DBCC UPDATEUSAGE (0)

--exec sp_createstats

PRINT 'EXEC sp_updatestats @resample = ''RESAMPLE'''
IF $(script_only) = 0		
	exec sp_updatestats @resample = 'RESAMPLE'


-- allow user to connect
PRINT 'ALTER DATABASE $(destdb) SET multi_user'
IF $(script_only) = 0		
	ALTER DATABASE $(destdb) SET multi_user

PRINT '-- DATABASE IS IN MULTI_USER MODE.'
PRINT '-- JOB FINISHED AT ' + cast(getdate() as varchar(40))
PRINT '-- '
PRINT 'Job passed success. Please pay attention on some errors raised during this process.'


PRINT '-- '
PRINT '-- '
PRINT '-- '
PRINT '-- '
PRINT '-- Start to check all views...'
PRINT CONVERT(varchar(30), getdate(), 109)

USE $(destdb)

DECLARE @view sysname

DECLARE cviews CURSOR FOR
SELECT DISTINCT schema_name(schema_id) + '.' + [name]
FROM sys.objects so 
WHERE [type] = 'V' 

OPEN cViews

FETCH cViews INTO @view

WHILE @@FETCH_STATUS = 0
BEGIN
	
	PRINT '-- Validation view name: ' + @view

	BEGIN TRY

		EXEC sp_refreshview @view

	END TRY

	BEGIN CATCH

		PRINT ERROR_MESSAGE() 

	END CATCH

	FETCH cViews INTO @view

END

CLOSE cViews
DEALLOCATE cViews


-- Recreat Columns with max
Declare Max_cur Cursor for 
select 'Alter Table '+Table_name+ '  Alter  Column ' +column_name +' '+Data_type+ '(Max)' from information_schema.columns
where Character_maximum_length=2001
and table_name like 'tbl%'

Open Max_cur
Fetch next from Max_cur into @stmt
Begin

EXEC sp_executesql @stmt

End
Close Max_cur
DeAllocate Max_cur








PRINT CONVERT(varchar(30), getdate(), 109)
PRINT '-- View validation passed...'
GO

use master
