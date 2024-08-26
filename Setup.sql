-- Dropping existing procedures and tables if they exist
USE tempdb
GO
SET NOCOUNT ON 
SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

DROP PROC dbo.Test28MB_Proc;
DROP PROC dbo.Test56MB_Proc;
DROP PROC dbo.Test224MB_Proc;
DROP PROC dbo.Test448MB_Proc;
DROP PROC Test448MB_Proc_Multi;
DROP TABLE Test28MB;
DROP TABLE Test56MB;
DROP TABLE Test224MB;
DROP TABLE Test448MB;
DROP TABLE SMT_Collector.LoadTests.ProctestRuns
DROP TABLE SMT_Collector.LoadTests.ProctestRuns_Waits
DROP TABLE SMT_Collector.LoadTests.CPUBench
DROP TABLE SMT_Collector.LoadTests.CPUBenchWaits
DROP TABLE SMT_Collector.LoadTests.QueryStatsPerTestRun
DROP VIEW dbo.veqs
DROP VIEW dbo.[vDbPtSt]
DROP VIEW dbo.vcol
DROP VIEW dbo.vOsBuf 
DROP VIEW dbo.vwPartition
DROP VIEW dbo.vtest
DROP TABLE dbo.zTest
DROP TABLE dbo.zTest2
DROP TABLE dbo.A
DROP TABLE dbo.A4
DROP TABLE dbo.A5
DROP TABLE dbo.A6
--Create table to hold Loadtest results. 
USE SMT_Collector
GO

CREATE TABLE SMT_Collector.LoadTests.[QueryStatsPerTestRun](
	[TestRun] [VARCHAR](256) NOT NULL,
	[query_hash] [BINARY](8) NULL,
	[statement_text] [NVARCHAR](MAX) NULL,
	[execution_count] [BIGINT] NOT NULL,
	[av_w] [NUMERIC](22, 2) NULL,
	[avg_elapsed_time] [BIGINT] NULL,
	[total_worker_time] [BIGINT] NOT NULL,
	[total_elapsed_time] [BIGINT] NOT NULL,
	[avg_worker_time] [BIGINT] NULL,
	[tphys_reads] [BIGINT] NOT NULL,
	[tlog_reads] [BIGINT] NOT NULL,
	[total_rows] [BIGINT] NULL,
	[start_off] [INT] NOT NULL,
	[end_off] [INT] NOT NULL,
	[creation_time] [DATETIME] NULL,
	[last_execution_time] [DATETIME] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO


CREATE TABLE SMT_Collector.LoadTests.[CPUBench](
	[TestRun] [VARCHAR](256) NULL,
	[Result] [BIGINT] NULL
) ON [PRIMARY]
GO

CREATE TABLE SMT_Collector.LoadTests.[CPUBenchWaits](
	[TestRun] [VARCHAR](256) NULL,
	[Wait_type] [NVARCHAR](256) NULL,
	[waiting_tasks_count] [BIGINT] NULL,
	[wait_time_ms] [BIGINT] NULL,
	[max_wait_time_ms] [BIGINT] NULL,
	[max_wait_diff_m_descr] [NVARCHAR](784) NULL,
	[signal_wait_time_ms] [BIGINT] NULL
) ON [PRIMARY]
GO

CREATE PARTITION FUNCTION SessionIDPartitionFunction (INT)
AS RANGE LEFT FOR VALUES (
    50, 51, 52, 53, 54, 55, 56, 57, 58, 59,
    60, 61, 62, 63, 64, 65, 66, 67, 68, 69,
    70, 71, 72, 73, 74, 75, 76, 77, 78, 79,
    80, 81, 82, 83, 84, 85, 86, 87, 88, 89,
    90, 91, 92, 93, 94, 95, 96, 97, 98, 99,
    100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
    110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
    120, 121, 122, 123, 124, 125, 126, 127, 128, 129,
    130, 131, 132, 133, 134, 135, 136, 137, 138, 139,
    140, 141, 142, 143, 144, 145, 146, 147, 148, 149,
    150, 151, 152, 153, 154, 155, 156, 157, 158, 159,
    160, 161, 162, 163, 164, 165, 166, 167, 168, 169,
    170, 171, 172, 173, 174, 175, 176, 177, 178, 179,
    180, 181, 182, 183, 184, 185, 186, 187, 188, 189,
    190, 191, 192, 193, 194, 195, 196, 197, 198, 199,
    200
);
GO
CREATE PARTITION SCHEME SessionIDPartitionScheme
AS PARTITION SessionIDPartitionFunction
ALL TO ([PRIMARY]);
GO

CREATE TABLE SMT_Collector.LoadTests.ProctestRuns
	(ID INT IDENTITY NOT NULL,
		TestRun NVARCHAR(128) NOT NULL,
		ProcName NVARCHAR(128) NOT NULL,
		SessionID INT  NOT NULL,
		ElapsedTime_us BIGINT NOT NULL,
		Res_Wtime_us BIGINT NOT NULL,
		Signal_WTime_us BIGINT NOT NULL,
		CPU_Time_us BIGINT NOT NULL,
		STAMP datetime2 NOT NULL
		)
ON SessionIDPartitionScheme(SessionID);
GO
CREATE CLUSTERED INDEX CL_ProctestRuns_TestRun_ProcName_SessionID_ID
ON SMT_Collector.LoadTests.ProctestRuns (TestRun,ProcName,SessionID,ID)--optimize for sequential insert?
GO
CREATE TABLE SMT_Collector.LoadTests.ProctestRuns_Waits
	(ID INT NOT NULL,--FK refference possible to LoadTests.ProctestRuns
		SessionID INT NOT NULL,
		wait_type NVARCHAR(256) NOT NULL ,
		waiting_tasks_count BIGINT NOT NULL,
		wait_time_ms BIGINT NOT NULL,
		max_wait_time_ms BIGINT NOT NULL,
		signal_wait_time_ms BIGINT NOT NULL,
		max_wait_diff_m_descr NVARCHAR(128) NOT NULL,
		STAMP DATETIME2 NOT NULL
		)
ON SessionIDPartitionScheme(SessionID);
GO
CREATE CLUSTERED INDEX CL_ProctestRuns_Waits_SessionID_ID
ON SMT_Collector.LoadTests.ProctestRuns_Waits (SessionID,ID)--optimize for sequential insert?
GO



USE tempdb;
GO
SET NOCOUNT ON 


/* Creating new tests based on your specified sizes */

/*28MB Test*/
CREATE TABLE Test28MB (Id INT IDENTITY, Payload CHAR(7996)); /*1 row 8KB*/ 
GO
/*3584 rows should be sufficient to create 28MB (3584 * 8KB = ~28MB)*/
INSERT Test28MB
VALUES ('A'); /*Repeat 3584 times*/ 
GO 3584

ALTER PROC dbo.Test28MB_Proc
@TestRun NVARCHAR(128)
AS
BEGIN
	SET NOCOUNT ON 

	DECLARE @ProcName NVARCHAR(128) = 'Test28MB_Proc'
	DECLARE @StartTime DATETIME2
	DECLARE @Wait_Time_ms BIGINT
	DECLARE @Signal_WTime_ms BIGINT
	DECLARE @LastInsertedID INT;

	DECLARE @Before AS TABLE (
								[session_id] [SMALLINT] NOT NULL,
								[wait_type] [NVARCHAR](256) NOT NULL,
								[waiting_tasks_count] [BIGINT] NOT NULL,
								[wait_time_ms] [BIGINT] NOT NULL,
								[max_wait_time_ms] [BIGINT] NOT NULL,
								[signal_wait_time_ms] [BIGINT] NOT NULL
								) 

	DECLARE @After AS TABLE (
								[session_id] [SMALLINT] NOT NULL,
								[wait_type] [NVARCHAR](256) NOT NULL,
								[waiting_tasks_count] [BIGINT] NOT NULL,
								[wait_time_ms] [BIGINT] NOT NULL,
								[max_wait_time_ms] [BIGINT] NOT NULL,
								[signal_wait_time_ms] [BIGINT] NOT NULL
								) 

	 /* Capture initial wait stats */
	INSERT @Before
	SELECT * 
	FROM sys.dm_exec_session_wait_stats WHERE session_id = @@SPID

	SET @StartTime  = SYSDATETIME()

	/* Actual Workload*/
	SELECT * FROM Test28MB 
	WHERE ID < 0 /*to touch the data but not send anything to client (prevent ASYNC_NETWORK_IO)*/
	OPTION (MAXDOP 1) ; 
	

	/*Collect Metrics*/
	DECLARE @ElapsedTime_us BIGINT = (SELECT DATEDIFF(MICROSECOND,@StartTime,SYSDATETIME()) )

	INSERT @After
	SELECT 
        session_id,
        wait_type,
        waiting_tasks_count,
        wait_time_ms,
        max_wait_time_ms,
        signal_wait_time_ms
	FROM sys.dm_exec_session_wait_stats 
	WHERE session_id = @@SPID

	/*Calculate wait times*/
	SELECT @Wait_Time_ms = SUM(COALESCE(a.wait_time_ms, b.wait_time_ms) - ISNULL(b.wait_time_ms, 0)),
			@Signal_WTime_ms = SUM(COALESCE(a.signal_wait_time_ms, b.signal_wait_time_ms) - ISNULL(b.signal_wait_time_ms, 0))
	FROM @Before  AS b  
		RIGHT JOIN  @After AS a
			ON a.wait_type = b.wait_type

	/*Insert overall test run results*/
	INSERT 	SMT_Collector.LoadTests.ProctestRuns
	SELECT  @TestRun,
			@ProcName,
			@@SPID,
			(@ElapsedTime_us) AS ElapsedTime_us ,
			(@Wait_Time_ms - @Signal_WTime_ms) *1000 AS Res_Wtime_us,
			@Signal_WTime_ms *1000 AS Signal_WTime_us,
			@ElapsedTime_us - (@Wait_Time_ms *1000) AS CPU_Time_us,
			SYSDATETIME()

	/*Capture the last inserted ID*/
	SET @LastInsertedID = SCOPE_IDENTITY();
	   
	/*Insert detailed wait stats*/
	INSERT SMT_Collector.LoadTests.ProctestRuns_Waits
	SELECT  @LastInsertedID
			, @@SPID
			, COALESCE (a.wait_type , b.wait_type) AS Wait_type
			, COALESCE(a.waiting_tasks_count, b.waiting_tasks_count) - ISNULL(b.waiting_tasks_count, 0)  AS waiting_tasks_count
			, COALESCE(a.wait_time_ms, b.wait_time_ms) - ISNULL(b.wait_time_ms, 0)  AS wait_time_ms
			, COALESCE(a.max_wait_time_ms, b.max_wait_time_ms)  AS max_wait_time_ms
			, COALESCE(a.signal_wait_time_ms, b.signal_wait_time_ms) - ISNULL(b.signal_wait_time_ms, 0)  AS signal_wait_time_ms	
			,'Diff: ' +CAST(COALESCE(a.max_wait_time_ms, b.max_wait_time_ms) - ISNULL(b.max_wait_time_ms, 0)AS nvarchar(256)) 
				+' Aft:' + CAST(COALESCE(a.max_wait_time_ms, 0 ) AS nvarchar(256))
				+' Bfr:'+ CAST(COALESCE (b.max_wait_time_ms,0) AS nvarchar(256)) max_wait_diff_m_descr		
			,SYSDATETIME()
	FROM @Before  AS b  
		RIGHT JOIN  @After AS a
			ON a.wait_type = b.wait_type	
END
GO

/*56MB Test*/ 
CREATE TABLE Test56MB (Id INT IDENTITY, Payload CHAR(7996)); /*1 row 8KB*/ 
GO
/*7168 rows should be sufficient to create 56MB (7168 * 8KB = ~56MB)*/ 
INSERT Test56MB
VALUES ('A'); /*Repeat 7168 times*/ 
GO 7168

ALTER PROC dbo.Test56MB_Proc
@TestRun NVARCHAR(128)
AS
BEGIN
	SET NOCOUNT ON 

	DECLARE @ProcName NVARCHAR(128) = 'Test56MB_Proc'
	DECLARE @StartTime DATETIME2
	DECLARE @Wait_Time_ms BIGINT
	DECLARE @Signal_WTime_ms BIGINT
	DECLARE @LastInsertedID INT;

	DECLARE @Before AS TABLE (
								[session_id] [SMALLINT] NOT NULL,
								[wait_type] [NVARCHAR](256) NOT NULL,
								[waiting_tasks_count] [BIGINT] NOT NULL,
								[wait_time_ms] [BIGINT] NOT NULL,
								[max_wait_time_ms] [BIGINT] NOT NULL,
								[signal_wait_time_ms] [BIGINT] NOT NULL
								) 

	DECLARE @After AS TABLE (
								[session_id] [SMALLINT] NOT NULL,
								[wait_type] [NVARCHAR](256) NOT NULL,
								[waiting_tasks_count] [BIGINT] NOT NULL,
								[wait_time_ms] [BIGINT] NOT NULL,
								[max_wait_time_ms] [BIGINT] NOT NULL,
								[signal_wait_time_ms] [BIGINT] NOT NULL
								) 

	 /* Capture initial wait stats */
	INSERT @Before
	SELECT * 
	FROM sys.dm_exec_session_wait_stats WHERE session_id = @@SPID

	SET @StartTime  = SYSDATETIME()

	/* Actual Workload*/
	SELECT * FROM Test56MB 
	WHERE ID < 0 /*to touch the data but not send anything to client (prevent ASYNC_NETWORK_IO)*/
	OPTION (MAXDOP 1) ; 

	/*Collect Metrics*/
	DECLARE @ElapsedTime_us BIGINT = (SELECT DATEDIFF(MICROSECOND,@StartTime,SYSDATETIME()))

	INSERT @After
	SELECT 
        session_id,
        wait_type,
        waiting_tasks_count,
        wait_time_ms,
        max_wait_time_ms,
        signal_wait_time_ms
	FROM sys.dm_exec_session_wait_stats 
	WHERE session_id = @@SPID

	/*Calculate wait times*/
	SELECT @Wait_Time_ms = SUM(COALESCE(a.wait_time_ms, b.wait_time_ms) - ISNULL(b.wait_time_ms, 0)),
			@Signal_WTime_ms = SUM(COALESCE(a.signal_wait_time_ms, b.signal_wait_time_ms) - ISNULL(b.signal_wait_time_ms, 0))
	FROM @Before  AS b  
		RIGHT JOIN  @After AS a
			ON a.wait_type = b.wait_type

	/*Insert overall test run results*/
	INSERT 	SMT_Collector.LoadTests.ProctestRuns
	SELECT  @TestRun,
			@ProcName,
			@@SPID,
			(@ElapsedTime_us) AS ElapsedTime_us ,
			(@Wait_Time_ms - @Signal_WTime_ms) *1000 AS Res_Wtime_us,
			@Signal_WTime_ms *1000 AS Signal_WTime_us,
			@ElapsedTime_us - (@Wait_Time_ms *1000) AS CPU_Time_us,
			SYSDATETIME()

	/*Capture the last inserted ID*/
	SET @LastInsertedID = SCOPE_IDENTITY();
	   
	/*Insert detailed wait stats*/
	INSERT SMT_Collector.LoadTests.ProctestRuns_Waits
	SELECT  @LastInsertedID
			, @@SPID
			, COALESCE (a.wait_type , b.wait_type) AS Wait_type
			, COALESCE(a.waiting_tasks_count, b.waiting_tasks_count) - ISNULL(b.waiting_tasks_count, 0)  AS waiting_tasks_count
			, COALESCE(a.wait_time_ms, b.wait_time_ms) - ISNULL(b.wait_time_ms, 0)  AS wait_time_ms
			, COALESCE(a.max_wait_time_ms, b.max_wait_time_ms)  AS max_wait_time_ms
			, COALESCE(a.signal_wait_time_ms, b.signal_wait_time_ms) - ISNULL(b.signal_wait_time_ms, 0)  AS signal_wait_time_ms	
			,'Diff: ' +CAST(COALESCE(a.max_wait_time_ms, b.max_wait_time_ms) - ISNULL(b.max_wait_time_ms, 0)AS nvarchar(256)) 
				+' Aft:' + CAST(COALESCE(a.max_wait_time_ms, 0 ) AS nvarchar(256))
				+' Bfr:'+ CAST(COALESCE (b.max_wait_time_ms,0) AS nvarchar(256)) max_wait_diff_m_descr			
			,SYSDATETIME()
	FROM @Before  AS b  
		RIGHT JOIN  @After AS a
			ON a.wait_type = b.wait_type
END
GO
/*28672 rows should be sufficient to create 224MB (28672 * 8KB = ~224MB)*/ 
INSERT Test224MB
VALUES ('A'); /* Repeat 28672 times*/
GO 28672

ALTER PROC dbo.Test224MB_Proc
@TestRun NVARCHAR(128)
AS
BEGIN
	SET NOCOUNT ON 

	DECLARE @ProcName NVARCHAR(128) = 'Test224MB_Proc'
	DECLARE @StartTime DATETIME2
	DECLARE @Wait_Time_ms BIGINT
	DECLARE @Signal_WTime_ms BIGINT
	DECLARE @LastInsertedID INT;

	DECLARE @Before AS TABLE (
								[session_id] [SMALLINT] NOT NULL,
								[wait_type] [NVARCHAR](256) NOT NULL,
								[waiting_tasks_count] [BIGINT] NOT NULL,
								[wait_time_ms] [BIGINT] NOT NULL,
								[max_wait_time_ms] [BIGINT] NOT NULL,
								[signal_wait_time_ms] [BIGINT] NOT NULL
								) 

	DECLARE @After AS TABLE (
								[session_id] [SMALLINT] NOT NULL,
								[wait_type] [NVARCHAR](256) NOT NULL,
								[waiting_tasks_count] [BIGINT] NOT NULL,
								[wait_time_ms] [BIGINT] NOT NULL,
								[max_wait_time_ms] [BIGINT] NOT NULL,
								[signal_wait_time_ms] [BIGINT] NOT NULL
								) 

	 /* Capture initial wait stats */
	INSERT @Before
	SELECT * 
	FROM sys.dm_exec_session_wait_stats WHERE session_id = @@SPID

	SET @StartTime  = SYSDATETIME()

	/* Actual Workload*/
	SELECT * FROM Test224MB 
	WHERE ID < 0 /*to touch the data but not send anything to client (prevent ASYNC_NETWORK_IO)*/
	OPTION (MAXDOP 1) ; 

	/*Collect Metrics*/
	DECLARE @ElapsedTime_us BIGINT = (SELECT DATEDIFF(MICROSECOND,@StartTime,SYSDATETIME()) )

	INSERT @After
	SELECT 
        session_id,
        wait_type,
        waiting_tasks_count,
        wait_time_ms,
        max_wait_time_ms,
        signal_wait_time_ms
	FROM sys.dm_exec_session_wait_stats 
	WHERE session_id = @@SPID

	/*Calculate wait times*/
	SELECT @Wait_Time_ms = SUM(COALESCE(a.wait_time_ms, b.wait_time_ms) - ISNULL(b.wait_time_ms, 0)),
			@Signal_WTime_ms = SUM(COALESCE(a.signal_wait_time_ms, b.signal_wait_time_ms) - ISNULL(b.signal_wait_time_ms, 0))
	FROM @Before  AS b  
		RIGHT JOIN  @After AS a
			ON a.wait_type = b.wait_type

	/*Insert overall test run results*/
	INSERT 	SMT_Collector.LoadTests.ProctestRuns
	SELECT  @TestRun,
			@ProcName,
			@@SPID,
			(@ElapsedTime_us) AS ElapsedTime_us ,
			(@Wait_Time_ms - @Signal_WTime_ms) *1000 AS Res_Wtime_us,
			@Signal_WTime_ms *1000 AS Signal_WTime_us,
			@ElapsedTime_us - (@Wait_Time_ms *1000) AS CPU_Time_us,
			SYSDATETIME()

	/*Capture the last inserted ID*/
	SET @LastInsertedID = SCOPE_IDENTITY();
	   
	/*Insert detailed wait stats*/
	INSERT SMT_Collector.LoadTests.ProctestRuns_Waits
	SELECT  @LastInsertedID
			, @@SPID
			, COALESCE (a.wait_type , b.wait_type) AS Wait_type
			, COALESCE(a.waiting_tasks_count, b.waiting_tasks_count) - ISNULL(b.waiting_tasks_count, 0)  AS waiting_tasks_count
			, COALESCE(a.wait_time_ms, b.wait_time_ms) - ISNULL(b.wait_time_ms, 0)  AS wait_time_ms
			, COALESCE(a.max_wait_time_ms, b.max_wait_time_ms)  AS max_wait_time_ms
			, COALESCE(a.signal_wait_time_ms, b.signal_wait_time_ms) - ISNULL(b.signal_wait_time_ms, 0)  AS signal_wait_time_ms	
			,'Diff: ' +CAST(COALESCE(a.max_wait_time_ms, b.max_wait_time_ms) - ISNULL(b.max_wait_time_ms, 0)AS nvarchar(256)) 
				+' Aft:' + CAST(COALESCE(a.max_wait_time_ms, 0 ) AS nvarchar(256))
				+' Bfr:'+ CAST(COALESCE (b.max_wait_time_ms,0) AS nvarchar(256)) max_wait_diff_m_descr		
			,SYSDATETIME()
	FROM @Before  AS b  
		RIGHT JOIN  @After AS a
			ON a.wait_type = b.wait_type
END
GO

/*448MB Test*/ 
CREATE TABLE Test448MB (Id INT IDENTITY, Payload CHAR(7996)); /*1 row 8KB*/ 
GO
/*57344 rows should be sufficient to create 448MB (57344 * 8KB = ~448MB)*/ 
INSERT Test448MB
VALUES ('A'); /* Repeat 57344 times*/
GO 57344


CREATE PROC dbo.Test448MB_Proc
@TestRun NVARCHAR(128)
AS
BEGIN
	SET NOCOUNT ON 

	DECLARE @ProcName NVARCHAR(128) = 'Test448MB_Proc'
	DECLARE @StartTime DATETIME2
	DECLARE @Wait_Time_ms BIGINT
	DECLARE @Signal_WTime_ms BIGINT
	DECLARE @LastInsertedID INT;

	DECLARE @Before AS TABLE (
								[session_id] [SMALLINT] NOT NULL,
								[wait_type] [NVARCHAR](256) NOT NULL,
								[waiting_tasks_count] [BIGINT] NOT NULL,
								[wait_time_ms] [BIGINT] NOT NULL,
								[max_wait_time_ms] [BIGINT] NOT NULL,
								[signal_wait_time_ms] [BIGINT] NOT NULL
								) 

	DECLARE @After AS TABLE (
								[session_id] [SMALLINT] NOT NULL,
								[wait_type] [NVARCHAR](256) NOT NULL,
								[waiting_tasks_count] [BIGINT] NOT NULL,
								[wait_time_ms] [BIGINT] NOT NULL,
								[max_wait_time_ms] [BIGINT] NOT NULL,
								[signal_wait_time_ms] [BIGINT] NOT NULL
								) 

	 /* Capture initial wait stats */
	INSERT @Before
	SELECT * 
	FROM sys.dm_exec_session_wait_stats WHERE session_id = @@SPID

	SET @StartTime  = SYSDATETIME()

	/* Actual Workload*/
	SELECT * FROM Test448MB 
	WHERE ID < 0 /*to touch the data but not send anything to client (prevent ASYNC_NETWORK_IO)*/
	OPTION (MAXDOP 1) ; 

	/*Collect Metrics*/
	DECLARE @ElapsedTime_us BIGINT = (SELECT DATEDIFF(MICROSECOND,@StartTime,SYSDATETIME()) ElapsedTime_us)

	INSERT @After
	SELECT 
        session_id,
        wait_type,
        waiting_tasks_count,
        wait_time_ms,
        max_wait_time_ms,
        signal_wait_time_ms
	FROM sys.dm_exec_session_wait_stats 
	WHERE session_id = @@SPID

	/*Calculate wait times*/
	SELECT @Wait_Time_ms = SUM(COALESCE(a.wait_time_ms, b.wait_time_ms) - ISNULL(b.wait_time_ms, 0)),
			@Signal_WTime_ms = SUM(COALESCE(a.signal_wait_time_ms, b.signal_wait_time_ms) - ISNULL(b.signal_wait_time_ms, 0))
	FROM @Before  AS b  
		RIGHT JOIN  @After AS a
			ON a.wait_type = b.wait_type

	/*Insert overall test run results*/
	INSERT 	SMT_Collector.LoadTests.ProctestRuns
	SELECT  @TestRun,
			@ProcName,
			@@SPID,
			(@ElapsedTime_us) AS ElapsedTime_us ,
			(@Wait_Time_ms - @Signal_WTime_ms) *1000 AS Res_Wtime_us,
			@Signal_WTime_ms *1000 AS Signal_WTime_us,
			@ElapsedTime_us - (@Wait_Time_ms *1000) AS CPU_Time_us,
			SYSDATETIME()

	/*Capture the last inserted ID*/
	SET @LastInsertedID = SCOPE_IDENTITY();
	   
	/*Insert detailed wait stats*/
	INSERT SMT_Collector.LoadTests.ProctestRuns_Waits
	SELECT  @LastInsertedID
			, @@SPID
			, COALESCE (a.wait_type , b.wait_type) AS Wait_type
			, COALESCE(a.waiting_tasks_count, b.waiting_tasks_count) - ISNULL(b.waiting_tasks_count, 0)  AS waiting_tasks_count
			, COALESCE(a.wait_time_ms, b.wait_time_ms) - ISNULL(b.wait_time_ms, 0)  AS wait_time_ms
			, COALESCE(a.max_wait_time_ms, b.max_wait_time_ms)  AS max_wait_time_ms
			, COALESCE(a.signal_wait_time_ms, b.signal_wait_time_ms) - ISNULL(b.signal_wait_time_ms, 0)  AS signal_wait_time_ms	
			,'Diff: ' +CAST(COALESCE(a.max_wait_time_ms, b.max_wait_time_ms) - ISNULL(b.max_wait_time_ms, 0)AS nvarchar(256)) 
				+' Aft:' + CAST(COALESCE(a.max_wait_time_ms, 0 ) AS nvarchar(256))
				+' Bfr:'+ CAST(COALESCE (b.max_wait_time_ms,0) AS nvarchar(256)) max_wait_diff_m_descr	
			,SYSDATETIME()
	FROM @Before  AS b  
		RIGHT JOIN  @After AS a
			ON a.wait_type = b.wait_type
END
GO
/*Parallelism test proc */
CREATE PROC dbo.Test448MB_Proc_Multi 
@TestRun NVARCHAR(128) , @MAXDOP CHAR(2) = '16'
AS
BEGIN
	SET NOCOUNT ON 

	DECLARE @ProcName NVARCHAR(128) = 'Test448MB_Proc_Multi'
	DECLARE @StartTime DATETIME2
	DECLARE @Wait_Time_ms BIGINT
	DECLARE @Signal_WTime_ms BIGINT
	DECLARE @LastInsertedID INT;

	DECLARE @Before AS TABLE (
								[session_id] [SMALLINT] NOT NULL,
								[wait_type] [NVARCHAR](256) NOT NULL,
								[waiting_tasks_count] [BIGINT] NOT NULL,
								[wait_time_ms] [BIGINT] NOT NULL,
								[max_wait_time_ms] [BIGINT] NOT NULL,
								[signal_wait_time_ms] [BIGINT] NOT NULL
								) 

	DECLARE @After AS TABLE (
								[session_id] [SMALLINT] NOT NULL,
								[wait_type] [NVARCHAR](256) NOT NULL,
								[waiting_tasks_count] [BIGINT] NOT NULL,
								[wait_time_ms] [BIGINT] NOT NULL,
								[max_wait_time_ms] [BIGINT] NOT NULL,
								[signal_wait_time_ms] [BIGINT] NOT NULL
								) 

	 /* Capture initial wait stats */
	INSERT @Before
	SELECT * 
	FROM sys.dm_exec_session_wait_stats WHERE session_id = @@SPID

	SET @StartTime  = SYSDATETIME()

	/* Actual Workload*/
	EXECUTE('SELECT * FROM Test448MB 
	WHERE ID < 0 /*to touch the data but not send anything to client (prevent ASYNC_NETWORK_IO)*/
	OPTION (MAXDOP '+@MAXDOP+') ;' )

	/*Collect Metrics*/
	DECLARE @ElapsedTime_us BIGINT = (SELECT DATEDIFF(MICROSECOND,@StartTime,SYSDATETIME()) ElapsedTime_us)

	INSERT @After
	SELECT 
        session_id,
        wait_type,
        waiting_tasks_count,
        wait_time_ms,
        max_wait_time_ms,
        signal_wait_time_ms
	FROM sys.dm_exec_session_wait_stats 
	WHERE session_id = @@SPID

	/*Calculate wait times*/
	SELECT @Wait_Time_ms = SUM(COALESCE(a.wait_time_ms, b.wait_time_ms) - ISNULL(b.wait_time_ms, 0)),
			@Signal_WTime_ms = SUM(COALESCE(a.signal_wait_time_ms, b.signal_wait_time_ms) - ISNULL(b.signal_wait_time_ms, 0))
	FROM @Before  AS b  
		RIGHT JOIN  @After AS a
			ON a.wait_type = b.wait_type

	/*Insert overall test run results*/
	INSERT 	SMT_Collector.LoadTests.ProctestRuns
	SELECT  @TestRun,
			@ProcName,
			@@SPID,
			(@ElapsedTime_us) AS ElapsedTime_us ,
			(@Wait_Time_ms - @Signal_WTime_ms) *1000 AS Res_Wtime_us,
			@Signal_WTime_ms *1000 AS Signal_WTime_us,
			(@ElapsedTime_us /*Total code runtime*/
				- (
					(@Wait_Time_ms /@MAXDOP) /*Average wait per thread*/
						*1000 /*convert to microsec*/
					)
				) 
				* @MAXDOP /*Account for all thread CPU util*/AS CPU_Time_us,
			SYSDATETIME()

	/*Capture the last inserted ID*/
	SET @LastInsertedID = SCOPE_IDENTITY();
	   
	/*Insert detailed wait stats*/
	INSERT SMT_Collector.LoadTests.ProctestRuns_Waits
	SELECT  @LastInsertedID
			, @@SPID
			, COALESCE (a.wait_type , b.wait_type) AS Wait_type
			, COALESCE(a.waiting_tasks_count, b.waiting_tasks_count) - ISNULL(b.waiting_tasks_count, 0)  AS waiting_tasks_count
			, COALESCE(a.wait_time_ms, b.wait_time_ms) - ISNULL(b.wait_time_ms, 0)  AS wait_time_ms
			, COALESCE(a.max_wait_time_ms, b.max_wait_time_ms)  AS max_wait_time_ms
			, COALESCE(a.signal_wait_time_ms, b.signal_wait_time_ms) - ISNULL(b.signal_wait_time_ms, 0)  AS signal_wait_time_ms	
			,'Diff: ' +CAST(COALESCE(a.max_wait_time_ms, b.max_wait_time_ms) - ISNULL(b.max_wait_time_ms, 0)AS nvarchar(256)) 
				+' Aft:' + CAST(COALESCE(a.max_wait_time_ms, 0 ) AS nvarchar(256))
				+' Bfr:'+ CAST(COALESCE (b.max_wait_time_ms,0) AS nvarchar(256)) max_wait_diff_m_descr	
			,SYSDATETIME()
	FROM @Before  AS b  
		RIGHT JOIN  @After AS a
			ON a.wait_type = b.wait_type
END
GO

/*Rebuild tables and Disable AUTO_UPDATE_STATISTICS to
prevent accidental requests for more logical reads*/
ALTER TABLE Test28MB REBUILD;
UPDATE STATISTICS Test28MB WITH NORECOMPUTE;
ALTER TABLE Test56MB REBUILD;
UPDATE STATISTICS Test56MB WITH NORECOMPUTE;
ALTER TABLE Test224MB REBUILD;
UPDATE STATISTICS Test224MB WITH NORECOMPUTE;
ALTER TABLE Test448MB REBUILD;
UPDATE STATISTICS Test448MB WITH NORECOMPUTE;


/* Further tests
Authored by Joe Chang 
https://www.linkedin.com/in/joe-chang-174a451/
*/

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER VIEW dbo.veqs AS
SELECT SUBSTRING(t.text, (q.statement_start_offset/2)+1, (CASE statement_end_offset WHEN -1 THEN DATALENGTH(t.text) ELSE q.statement_end_offset END - q.statement_start_offset)/2 + 1) AS statement_text
, execution_count, total_worker_time, total_elapsed_time
, total_physical_reads tphys_reads, total_logical_reads tlog_reads, total_rows
, statement_start_offset start_off, statement_end_offset end_off
, CASE execution_count WHEN 0 THEN 0 ELSE (100*total_worker_time/execution_count)*0.01 END av_w
, max_worker_time, max_elapsed_time, max_dop
, q.creation_time, q.last_execution_time, q.query_hash
FROM sys.dm_exec_query_stats q OUTER APPLY sys.dm_exec_sql_text(q.sql_handle) as t
GO
CREATE OR ALTER VIEW dbo.[vDbPtSt] AS
WITH a AS (
 SELECT object_id, index_id, SUM(in_row_data_page_count) idp, SUM(in_row_used_page_count) iup, SUM(in_row_reserved_page_count) irp, SUM(row_count) row_count
 , SUM(lob_used_page_count) lup, SUM(row_overflow_used_page_count) oup, SUM(CASE row_count WHEN 0 THEN 0 ELSE 1 END) pp, COUNT(*) pt
 FROM sys.dm_db_partition_stats WITH(NOLOCK) WHERE object_id > 1000 GROUP BY object_id, index_id 
) , d AS (
 SELECT object_id, index_id, idp, iup, irp, row_count, lup, oup, pp, pt 
 , CASE idp WHEN 0 THEN 0 ELSE 1.0*row_count/idp END Rw_Pg
 , CASE row_count WHEN 0 THEN 0 ELSE 8096.*idp/row_count END ABR
 , CASE row_count WHEN 0 THEN 1 ELSE CONVERT(int,8096.*idp/row_count) END IBR
 FROM a
)
SELECT o.name [table], i.name [index], d.index_id, i.data_space_id dsid, idp, iup, irp, row_count, o.object_id, o.type
, CONVERT(decimal(19,2),d.Rw_Pg) AS Rw_Pg, CONVERT(decimal(19,2),d.ABR) AS ABR, d.pt, d.pp, d.lup, d.oup
, u.user_seeks, u.user_scans, u.user_lookups, u.user_updates , INDEXPROPERTY(o.object_id, i.name,'IndexDepth') IxDep 
, 8096/IBR RpP, 8096-(8096/IBR)*IBR FpP
, (8096 - (row_count % (8096/IBR))*CONVERT(int,ABR)) % 8096 AS LstPFr , row_count % (8096/IBR) AS lstPRw 
, CONVERT(decimal(19,1), 8096*idp - row_count*ABR) AS altLPg
FROM d JOIN sys.indexes i ON i.object_id = d.object_id AND i.index_id = d.index_id
JOIN sys.objects o ON o.object_id = i.object_id 
LEFT JOIN sys.dm_db_index_usage_stats u ON u.database_id = DB_ID() AND u.object_id = d.object_id AND u.index_id = d.index_id
WHERE o.type <> 'IT' 
GO
CREATE OR ALTER VIEW dbo.vcol AS
WITH x AS (
 SELECT o.object_id, o.schema_id, o.name, o.type, b.idp, b.row_count, b.index_id, 8096*b.idp tBytes
 , CASE b.row_count WHEN 0 THEN 0. ELSE 8096.*b.idp/b.row_count END ABR
 , CASE b.row_count WHEN 0 THEN 1 ELSE CONVERT(int,8096.*b.idp/b.row_count) END IBR
 FROM sys.objects o
 OUTER APPLY (
	SELECT d.index_id, SUM(in_row_data_page_count) idp, SUM(row_count) row_count
	FROM sys.dm_db_partition_stats d WITH(NOLOCK) 
	WHERE d.object_id = o.object_id AND d.index_id <= 1 GROUP BY d.index_id
 ) b  WHERE o.type = 'U'
) 
SELECT x.object_id, x.schema_id, x.name, x.type, x.idp, x.row_count, x.index_id, a.col, a.byt, j.ix
, CONVERT(decimal(9,2),x.ABR) ABR , 8096/IBR RpP
, 8096-(8096/IBR)*IBR FpP
, (8096 - (row_count % (8096/IBR))*CONVERT(int,ABR)) % 8096 AS LstPFr , row_count % (8096/IBR) AS lstPRw 
, CONVERT(decimal(19,1),8096*idp - row_count*ABR) AS altLPg
, IBR                
, (8096/IBR)*IBR ByUs
--, (8096./IBR)*IBR ByUs
FROM x
CROSS APPLY ( SELECT COUNT(*) col, SUM(max_length) byt FROM sys.columns c WHERE c.object_id = x.object_id GROUP BY c.object_id ) a
OUTER APPLY ( SELECT COUNT(*) ix FROM sys.indexes i WHERE i.object_id = x.object_id AND i.index_id > 1 GROUP BY i.object_id ) j
--WHERE x.type = 'U'
--, tBytes - CONVERT(int,ABR)*row_count uBytes
--, row_count / (8096/IBR) fullP
-- CONVERT(decimal(18,4),(8096.-FpP)/RpP) 
GO
CREATE OR ALTER VIEW dbo.vOsBuf AS
SELECT p.object_id, o.name
, b.file_id, b.page_id, b.page_level, b.page_type, b.row_count, b.free_space_in_bytes
, b.numa_node, b.read_microsec, b.is_modified
, a.type, a.data_space_id, a.data_pages
, a.total_pages, a.used_pages
, p.rows, p.index_id
FROM sys.dm_os_buffer_descriptors b WITH(NOLOCK) 
JOIN sys.allocation_units a ON a.allocation_unit_id = b.allocation_unit_id
JOIN sys.partitions p WITH(NOLOCK) ON a.container_id = CASE a.type WHEN 2 THEN p.partition_id ELSE p.hobt_id END
JOIN sys.objects o WITH(NOLOCK) ON o.object_id = p.object_id
WHERE b.database_id = DB_ID()
--AND o.name = 'Nums' --AND page_type = 'DATA_PAGE'
--AND b.row_count <> 100
--ORDER BY b.database_id, b.file_id, b.page_id
GO
CREATE OR ALTER VIEW dbo.vwPartition
AS
 SELECT i.object_id, i.index_id, u.name sch, o.name tabl, i.name [indx]
 , f.name pfn, f.function_id
 , s.name psn, i.data_space_id psi
 , d.partition_number pn
 , r.value
 , d.in_row_data_page_count page_cnt
 , d.row_overflow_used_page_count ovr_cnt
 , d.lob_used_page_count lob_cnt
 , d.reserved_page_count res_cnt
 , d.row_overflow_reserved_page_count ovr_res
 , d.lob_reserved_page_count lob_res
 , d.row_count row_cnt
 , CASE d.row_count WHEN 0 THEN 0 ELSE CONVERT(decimal(18,1),(8192.*d.in_row_data_page_count)/d.row_count) END RwSz
 , CASE d.row_count WHEN 0 THEN 0 ELSE CONVERT(decimal(18,1),(8192.*d.lob_used_page_count)/d.row_count) END LbSz
 , e.data_space_id dsid
 , p.data_compression cmp
 , i.fill_factor ff
 FROM sys.indexes i WITH(NOLOCK) 
 INNER JOIN sys.objects o WITH(NOLOCK) ON o.object_id = i.object_id
 JOIN sys.schemas u ON u.schema_id = o.schema_id
 INNER JOIN sys.dm_db_partition_stats d WITH(NOLOCK) ON d.object_id = i.object_id AND d.index_id = i.index_id 
 LEFT JOIN sys.partition_schemes s WITH(NOLOCK) ON s.data_space_id = i.data_space_id  
 LEFT JOIN sys.partition_functions f WITH(NOLOCK) ON f.function_id = s.function_id
 LEFT JOIN sys.destination_data_spaces e WITH(NOLOCK) ON e.partition_scheme_id = i.data_space_id AND e.destination_id = d.partition_number 
 LEFT JOIN sys.partition_range_values r WITH(NOLOCK) ON r.function_id = s.function_id AND r.boundary_id = e.destination_id - f.boundary_value_on_right
 LEFT JOIN sys.partitions p WITH(NOLOCK) ON p.object_id = d.object_id AND p.index_id = d.index_id AND p.partition_number = d.partition_number
 WHERE i.type IN(0,1,2,5) AND i.is_disabled = 0 AND i.is_hypothetical = 0
GO

CREATE OR ALTER VIEW dbo.vtest AS
WITH a AS (
 SELECT object_id, index_id
 , SUM(in_row_data_page_count) idp
 , SUM(in_row_used_page_count) iup
 , SUM(in_row_reserved_page_count) irp
 , SUM(row_count) row_count
 , SUM(lob_used_page_count) lup
 , SUM(row_overflow_used_page_count) oup
 , SUM(CASE row_count WHEN 0 THEN 0 ELSE 1 END) pp, COUNT(*) pt
 FROM sys.dm_db_partition_stats WITH(NOLOCK) WHERE object_id > 1000 AND index_id <= 1
 GROUP BY object_id, index_id 
) , d AS (
 SELECT object_id/*, index_id*/, idp, iup, irp, row_count, lup, oup, pp, pt 
 , CASE idp WHEN 0 THEN 0 ELSE 1.0*row_count/idp END Rw_Pg
 , CASE row_count WHEN 0 THEN 0 ELSE 8096.*idp/row_count END ABR
 , CASE row_count WHEN 0 THEN 1 ELSE CONVERT(int,8096.*idp/row_count) END IBR
 FROM a
)
SELECT o.name [table], row_count --, i.name [index], d.index_id
, d.row_count/h.ubd Cardin, h.ubd Rang
, idp, iup, irp, o.object_id, o.type
, CONVERT(decimal(19,2),d.Rw_Pg) AS Rw_Pg
, CONVERT(decimal(19,2),d.ABR) AS ABR
, d.pt, d.pp, d.lup, d.oup
, u.user_seeks, u.user_scans, u.user_lookups, u.user_updates
, INDEXPROPERTY(o.object_id, i.name,'IndexDepth') IxDep 
, 8096/IBR RpP, 8096-(8096/IBR)*IBR FpP
, (8096 - (row_count % (8096/IBR))*CONVERT(int,ABR)) % 8096 AS LstPFr 
, row_count % (8096/IBR) AS lstPRw 
, CONVERT(decimal(19,1), 8096*idp - row_count*ABR) AS altLPg
, i.data_space_id dsid
FROM d 
LEFT JOIN sys.indexes i ON i.object_id = d.object_id AND i.index_id IN (0,1) -- d.index_id
JOIN sys.objects o ON o.object_id = d.object_id 
LEFT JOIN sys.dm_db_index_usage_stats u ON u.database_id = DB_ID() AND u.object_id = i.object_id AND u.index_id = i.index_id
OUTER APPLY (
 SELECT MIN(h.range_high_key) lbd , CONVERT(bigint,MAX(h.range_high_key)) ubd , SUM(h.range_rows) + SUM(h.equal_rows) srw 
 FROM sys.dm_db_stats_histogram (o.object_id, 1) h
) h
WHERE o.type IN ('U', 'V') -- <> 'IT' 
--AND o.name LIKE 'A%' AND o.name <> 'A'
--ORDER BY o.name
GO


-- Sets the tables to be created with populate statistics
IF NOT EXISTS(SELECT * FROM sys.objects WHERE object_id = OBJECT_ID('dbo.zTest'))
CREATE TABLE dbo.zTest(Tabl varchar(250) NOT NULL
, Cardin int NOT NULL -- number of rows for given GID value, Max SID
, Rang int NOT NULL -- range of GID values
, RowCt int NOT NULL -- total rows = Cardinality x Range
, ird int NOT NULL -- in row data pages = RowCt / 101 -- not used
, Loop1 int, Loop2 int, Merg int, Hasj int, SMer int, Mtm int  -- number of iterations to run
)
CREATE UNIQUE CLUSTERED INDEX UCX ON dbo.zTest(Cardin)
-- Comet
IF 1=2
 TRUNCATE TABLE dbo.zTest
IF NOT EXISTS(SELECT * FROM dbo.zTest)
INSERT dbo.zTest(Tabl,Cardin,Rang,RowCt,ird,Loop1,Loop2,Merg,Hasj,SMer,Mtm) VALUES 
 ('A',   1, 6630044,6630044,65644,200000,200000,200000,35000,0,0)
,('A1',  2, 681800,1363600,13501,200000,200000,200000,30000,130000,0)
,('A1A', 3, 454533,1363599,13501,200000,200000,200000,29000,127000,0)
,('A2',  4, 340900,1363600,13501,180000,200000,200000,28000,125000,0)
,('A2A', 6, 227266,1363596,13501,160000,200000,200000,27500,120000,0)
,('A3',  8, 170450,1363600,13501,150000,200000,200000,27000,115000,0)
,('A3A', 12,113633,1363596,13501,135000,180000,200000,26500,112000,0)
,('A4',  16, 85225,1363600,13501,120000,160000,200000,26000,110000,0)
,('A4A', 24, 56816,1363584,13501,100000,130000,200000,25500,95000,0)
,('A5',  32, 42612,1363584,13501,80000,100000,190000,25000,85000,0)
,('A5A', 48, 28408,1363584,13501,60000,80000,160000,24500,80000,0)
,('A6',  64, 21306,1363584,13501,50000,65000,140000,24000,70000,0)
,('A6A', 96, 14204,1363584,13501,35000,50000,110000,22000,60000,0)
,('A7',  128,10653,1363584,13501,25000,35000,90000,20000,55000,0)
,('A7A', 192, 7102,1363584,13501,20000,27000,70000,18000,45000,0)
,('A8',  256, 5326,1363456,13500,15000,20000,50000,16000,35000,0)
,('A8B', 320, 4261,1363520,13501,13000,17000,46000,15500,30000,0)
,('A8C', 336, 4058,1363488,13500,12000,16000,44000,15000,29000,0)
,('A8D', 352, 3874,1363648,13502,11000,15000,42000,14500,28000,0)
,('A8A', 384, 3551,1363584,13501,10000,14000,40000,14000,27000,0)
,('A9',  512, 2663,1363456,13500,7500,10000,30000,12000,20000,0)
,('A9A', 768, 1775,1363200,13498,6000,7000,23000,10000,15000,0)
,('AA',  1024,1331,1362944,13495,4000,5000,17000,8000,11000,0)
,('AA1', 1536, 887,1362432,13490,3000,3500,13000,6500,8500,0)
,('AB',  2048, 665,1361920,13485,1850,2500,8000,4500,6000,0)
,('AB1', 3072, 443,1360896,13475,1500,1900,6500,3500,4500,0)
,('AC',  4096, 332,1359872,13465,1000,1300,4500,2500,3200,0)
,('AC1', 5120, 266,1361920,13485,800,1000,3500,2000,2500,0)
,('AD',  8192, 202,1654784,16384,500,650,2100,1260,1500,0)
,('AD1', 8704, 202,1758208,17408,480,600,2050,1250,1470,0)
,('AD2', 8832, 202,1784064,17664,440,595,2030,1230,1450,0)
,('AD5', 8890, 202,1795780,17780,440,585,2000,1200,1410,0)
,('AD6', 8891, 202,1795982,17782,440,590,2020,1220,1420,0)
,('AD8', 8960, 202,1809920,17920,430,570,2010,1200,1380,0)
,('AE',  9216, 202,1861632,18432,420,550,2000,1120,1320,0)
,('AE1', 9728, 202,1965056,19456,400,520,1900,1100,1300,0)
,('AE2', 10000,202,2020000,20000,390,510,1800,1020,1240,0)
,('AE3', 10240,202,2068480,20480,380,500,1700,1000,1200,0)
,('AE4', 12288,202,2482176,24576,320,420,1600,850,1000,0)
,('AE5', 14336,202,2895872,28672,280,350,1300,700,900,0)
,('AF',  16384,404,6619136,65536,230,320,1200,600,800,0)
,('AF1', 24576,269,6610944,65455,150,210,700,420,500,0)
,('AG',  32768,202,6619136,65536,120,160,600,320,400,0)
,('AG1', 49152,134,6586368,65212,80,110,400,220,260,0)
,('AH',  65536,101,6619136,65536,60,80,300,160,200,0)
,('AH1', 98304, 67,6586368,65212,45,55,200,110,140,0)
,('AH2',114688, 57,6537216,64725,40,50,180,90,120,0)
,('AH4',130048, 50,6502400,64381,37,40,150,80,105,0)
,('AH5',131071, 50,6553550,64887,36,35,140,75,95,0)
,('AI', 131072, 50,6553600,64888,35,40,145,80,100,0)
,('AI1',163840, 40,6553600,64888,33,35,120,55,80,0)
,('AI2',196608, 33,6488064,64239,32,32,110,50,70,0)
,('AI3',229376, 28,6422528,63590,31,31,105,42,60,0)
,('AJ', 262144, 25,6553600,64888,30,30,100,40,50,0)
GO

IF NOT EXISTS(SELECT * FROM sys.objects WHERE object_id = OBJECT_ID('dbo.zTest2'))
CREATE TABLE dbo.zTest2(Tabl varchar(250) NOT NULL
, Cardin int NOT NULL -- number of rows for given GID value, Max SID
, Rang int NOT NULL -- range of GID values
, RowCt int NOT NULL -- total rows = Cardinality x Range
, ird int NOT NULL -- in row data pages = RowCt / 101 -- not used
, Loop1 int, Loop2 int, Merg int, Hasj int, SMer int, Mtm int  -- number of iterations to run
)
CREATE UNIQUE CLUSTERED INDEX UCX ON dbo.zTest2(Cardin)
-- Rocket
IF 1=2
 TRUNCATE TABLE dbo.zTest2
IF NOT EXISTS(SELECT * FROM dbo.zTest2)
INSERT dbo.zTest2(Tabl,Cardin,Rang,RowCt,ird,Loop1,Loop2,Merg,Hasj,SMer,Mtm) VALUES 
 ('A',   1, 6630044,6630044,65644,200000,200000,200000,42000,0,0)
,('A1',  2, 681800,1363600,13501,200000,200000,200000,40000,175000,0)
,('A1A', 3, 454533,1363599,13501,200000,200000,200000,38000,170000,0)
,('A2',  4, 340900,1363600,13501,200000,200000,200000,37000,165000,0)
,('A2A', 6, 227266,1363596,13501,200000,200000,200000,36500,162000,0)
,('A3',  8, 170450,1363600,13501,200000,200000,200000,36000,160000,0)
,('A3A', 12,113633,1363596,13501,200000,200000,200000,35500,155000,0)
,('A4',  16, 85225,1363600,13501,150000,200000,200000,35000,150000,0)
,('A4A', 24, 56816,1363584,13501,120000,170000,200000,34000,140000,0)
,('A5',  32, 42612,1363584,13501,100000,140000,200000,33000,130000,0)
,('A5A', 48, 28408,1363584,13501,80000,110000,200000,32000,120000,0)
,('A6',  64, 21306,1363584,13501,60000,85000,185000,31000,110000,0)
,('A6A', 96, 14204,1363584,13501,45000,60000,150000,29000,90000,0)
,('A7',  128,10653,1363584,13501,35000,43000,120000,27000,75000,0)
,('A7A', 192, 7102,1363584,13501,27000,33000,90000,24000,60000,0)
,('A8',  256, 5326,1363456,13500,20000,25000,70000,22000,50000,0)
,('A8B', 320, 4261,1363520,13500,18000,21000,65000,21000,47000,0)
,('A8C', 336, 4058,1363488,13500,17000,20000,60000,20000,45000,0)
,('A8D', 352, 3874,1363648,13502,16000,19000,55000,19500,43000,0)
,('A8A', 384, 3551,1363584,13500,15000,18000,50000,19000,40000,0)
,('A9',  512, 2663,1363456,13500,10000,13000,40000,17000,30000,0)
,('A9A', 768, 1775,1363200,13497,7000,10000,30000,14000,22000,0)
,('AA',  1024,1331,1362944,13495,5000,6700,22000,10000,15000,0)
,('AA1', 1536, 887,1362432,13490,3700,5000,17000,8000,13000,0)
,('AB',  2048, 665,1361920,13485,2500,3500,12000,6500,9000,0)
,('AB1', 3072, 443,1360896,13475,1800,2500,9000,5000,7000,0)
,('AC',  4096, 332,1359872,13465,1200,1700,6000,3500,4500,0)
,('AC1', 5120, 266,1361920,13485,1000,1400,5000,3000,3500,0)
,('AD',  8192, 202,1654784,16384,700,900,3200,1850,2200,0)
,('AD1', 8704, 202,1758208,17408,650,850,3000,1820,2100,0)
,('AD2', 8832, 202,1784064,17664,645,820,2850,1780,2000,0)
,('AD5', 8890, 202,1795780,17780,635,810,2800,1760,1980,0)
,('AD6', 8891, 202,1795982,17782,640,820,2850,1770,1990,0)
,('AD8', 8960, 202,1809920,17920,610,800,2800,1700,1970,0)
,('AE',  9216, 202,1861632,18432,590,780,2780,1600,1920,0)
,('AE1', 9728, 202,1965056,19456,560,750,2600,1550,1800,0)
,('AE2', 10000,202,2020000,20000,540,720,2500,1500,1780,0)
,('AE3', 10240,202,2068480,20480,520,700,2400,1450,1760,0)
,('AE4', 12288,202,2482176,24576,450,600,2100,1200,1500,0)
,('AE5', 14336,202,2895872,28672,400,500,1800,1000,1300,0)
,('AF',  16384,404,6619136,65536,300,450,1500,900,1100,0)
,('AF1', 24576,269,6610944,65454,200,300,1100,600,750,0)
,('AG',  32768,202,6619136,65536,160,220,800,450,600,0)
,('AG1', 49152,134,6586368,65211,110,150,520,300,400,0)
,('AH',  65536,101,6619136,65536,90,120,400,220,300,0)
,('AH1', 98304, 67,6586368,65212,60,75,300,150,200,0)
,('AH2',114688, 57,6537216,64725,50,65,250,130,180,0)
,('AH4',130048, 50,6502400,64380,45,60,210,110,150,0)
,('AH5',131071, 50,6553550,64886,40,55,200,110,140,0)
,('AI', 131072, 50,6553600,64888,45,60,200,110,140,0)
,('AI1',163840, 40,6553600,64887,35,45,170,75,110,0)
,('AI2',196608, 33,6488064,64238,32,40,150,70,100,0)
,('AI3',229376, 28,6422528,63589,31,35,120,55,90,0)
,('AJ', 262144, 25,6553600,64888,30,30,110,50,80,0)

GO

IF NOT EXISTS (
 SELECT * FROM sys.objects WHERE name = 'Nums'
) CREATE TABLE dbo.Nums (I int NOT NULL) --ON FG2
GO
SET NOCOUNT ON
DECLARE @I int = 1
BEGIN TRAN
WHILE (@I <= 10574) BEGIN -- 10574
 INSERT dbo.Nums VALUES(@I)
 SET @I = @I + 1
END
COMMIT TRAN
SELECT @@TRANCOUNT TranCt
GO

IF NOT EXISTS (
 SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('Nums') AND index_id = 1
) CREATE UNIQUE CLUSTERED INDEX UCX ON dbo.Nums(I) WITH (SORT_IN_TEMPDB = ON, FILLFACTOR = 100, MAXDOP = 1) ON [PRIMARY]
ELSE 
 ALTER INDEX ALL ON Nums REBUILD


IF NOT EXISTS(SELECT * FROM sys.objects WHERE object_id = OBJECT_ID('dbo.A'))
 CREATE TABLE dbo.A(AID INT NOT NULL, GID INT NOT NULL, [SID] INT NOT NULL, BID INT NOT NULL, CID INT NOT NULL, DID INT NOT NULL, EID INT NOT NULL
 , R1 INT NOT NULL, R2 INT NOT NULL, R3 INT NOT NULL, R4 INT NOT NULL, R5 INT NOT NULL, R6 INT NOT NULL, R7 INT NOT NULL, R8 INT NOT NULL, R9 INT NOT NULL, RA INT NOT NULL, TI TINYINT NOT NULL
 , CONSTRAINT PK_A PRIMARY KEY CLUSTERED(AID))

IF NOT EXISTS(SELECT * FROM sys.objects WHERE object_id = OBJECT_ID('dbo.A4'))
 CREATE TABLE dbo.A4(AID INT NOT NULL, GID INT NOT NULL, [SID] INT NOT NULL, BID INT NOT NULL, CID INT NOT NULL, DID INT NOT NULL, EID INT NOT NULL
 , R1 INT NOT NULL, R2 INT NOT NULL, R3 INT NOT NULL, R4 INT NOT NULL, R5 INT NOT NULL, R6 INT NOT NULL, R7 INT NOT NULL, R8 INT NOT NULL, R9 INT NOT NULL, RA INT NOT NULL, TI TINYINT NOT NULL
 , CONSTRAINT PK_A4 PRIMARY KEY CLUSTERED(GID,SID))

IF NOT EXISTS(SELECT * FROM sys.objects WHERE object_id = OBJECT_ID('dbo.A5'))
 CREATE TABLE dbo.A5(AID INT NOT NULL, GID INT NOT NULL, [SID] INT NOT NULL, BID INT NOT NULL, CID INT NOT NULL, DID INT NOT NULL, EID INT NOT NULL
 , R1 INT NOT NULL, R2 INT NOT NULL, R3 INT NOT NULL, R4 INT NOT NULL, R5 INT NOT NULL, R6 INT NOT NULL, R7 INT NOT NULL, R8 INT NOT NULL, R9 INT NOT NULL, RA INT NOT NULL, TI TINYINT NOT NULL
 , CONSTRAINT PK_A5 PRIMARY KEY CLUSTERED(GID,SID))

IF NOT EXISTS(SELECT * FROM sys.objects WHERE object_id = OBJECT_ID('dbo.A6'))
 CREATE TABLE dbo.A6(AID INT NOT NULL, GID INT NOT NULL, [SID] INT NOT NULL, BID INT NOT NULL, CID INT NOT NULL, DID INT NOT NULL, EID INT NOT NULL
 , R1 INT NOT NULL, R2 INT NOT NULL, R3 INT NOT NULL, R4 INT NOT NULL, R5 INT NOT NULL, R6 INT NOT NULL, R7 INT NOT NULL, R8 INT NOT NULL, R9 INT NOT NULL, RA INT NOT NULL, TI TINYINT NOT NULL
 , CONSTRAINT PK_A6 PRIMARY KEY CLUSTERED(GID,SID))
GO

DECLARE @I int = 0, @N int = 10574, @Card int = 1, @Rn int = 19857408, @T int = 19857408, @Rh int; 
DECLARE @R1 int=2, @R2 int=4, @R3 int=8, @R4 int=16, @R5 int=32, @R6 int=64, @R7 int=128, @R8 int=256, @R9 int=512, @RA int=1024, @RY int=250
IF NOT EXISTS(SELECT * FROM dbo.A)
BEGIN
 SELECT @Rh = Rang FROM dbo.zTest WHERE Cardin = 2
 SELECT @Rn = CASE @Card WHEN 1 THEN @Rh ELSE @Rn END
WHILE @I*@N < @T
BEGIN
 INSERT dbo.A
 SELECT ID+1 ID, G, S, B, (S-1)%@R1+1 C, (S-1)%@R2+1 D, (S-1)%@R3+1 E
 , ID%@R1+1 R1, ID%@R2+1 R2, ID%@R3+1 R3, ID%@R4+1 R4, ID%@R5+1 R5, ID%@R6+1 R6, ID%@R7+1 R7, ID%@R8+1 R8, ID%@R9+1 R9, ID%@RA+1 RA, ID%@RY+1 GT
 FROM (SELECT ID ID, ID%@Rn+1 G, ID/@Rn+1 S, ID/@Rn+1 B FROM (SELECT @I*@N+I-1 ID FROM dbo.Nums) a ) b WHERE ID < @T; 
 SELECT @I = @I+1
END 
ALTER INDEX ALL ON dbo.A REBUILD WITH(SORT_IN_TEMPDB=ON, MAXDOP=1, FILLFACTOR=100)
END 
GO

CREATE STATISTICS ST_GID ON dbo.A(GID) WITH FULLSCAN
CREATE STATISTICS ST_SID ON dbo.A([SID]) WITH FULLSCAN
CREATE STATISTICS ST_BID ON dbo.A(BID) WITH FULLSCAN
CREATE STATISTICS ST_CID ON dbo.A(CID) WITH FULLSCAN

GO
SET NOCOUNT ON; 
DECLARE @I INT = 0, @N INT = 10574, @Card INT = 16, @Rn INT = 1241088, @T INT = 19857408; 
DECLARE @R1 INT=2, @R2 INT=4, @R3 INT=8, @R4 INT=16, @R5 INT=32, @R6 INT=64, @R7 INT=128, @R8 INT=256, @R9 INT=512, @RA INT=1024, @RY INT=250
IF NOT EXISTS(SELECT * FROM dbo.A4)
BEGIN
WHILE @I*@N < @T
BEGIN
 INSERT dbo.A4
 SELECT ID+1 ID, G, S, B, (S-1)%@R1+1 C, (S-1)%@R2+1 D, (S-1)%@R3+1 E
 , ID%@R1+1 R1, ID%@R2+1 R2, ID%@R3+1 R3, ID%@R4+1 R4, ID%@R5+1 R5, ID%@R6+1 R6, ID%@R7+1 R7, ID%@R8+1 R8, ID%@R9+1 R9, ID%@RA+1 RA, ID%@RY+1 GT
 FROM (SELECT ID ID, ID%@Rn+1 G, ID/@Rn+1 S, ID/@Rn+1 B FROM (SELECT @I*@N+I-1 ID FROM dbo.Nums) a ) b WHERE ID < @T; 
 SELECT @I = @I+1
END 
ALTER INDEX ALL ON dbo.A4 REBUILD WITH(SORT_IN_TEMPDB=ON, MAXDOP=1, FILLFACTOR=100)
END 
GO
CREATE STATISTICS ST_AID ON dbo.A4(AID) WITH FULLSCAN
CREATE STATISTICS ST_SID ON dbo.A4([SID]) WITH FULLSCAN

GO
SET NOCOUNT ON; 
DECLARE @I int = 0, @N int = 10574, @Card int = 16, @Rn int = 310272, @T int = 4964352; 
DECLARE @R1 int=2, @R2 int=4, @R3 int=8, @R4 int=16, @R5 int=32, @R6 int=64, @R7 int=128, @R8 int=256, @R9 int=512, @RA int=1024, @RY int=250
IF NOT EXISTS(SELECT * FROM dbo.A5)
BEGIN
WHILE @I*@N < @T
BEGIN
 INSERT dbo.A5
 SELECT ID+1 ID, G, S, B, (S-1)%@R1+1 C, (S-1)%@R2+1 D, (S-1)%@R3+1 E
 , ID%@R1+1 R1, ID%@R2+1 R2, ID%@R3+1 R3, ID%@R4+1 R4, ID%@R5+1 R5, ID%@R6+1 R6, ID%@R7+1 R7, ID%@R8+1 R8, ID%@R9+1 R9, ID%@RA+1 RA, ID%@RY+1 GT
 FROM (SELECT ID ID, ID%@Rn+1 G, ID/@Rn+1 S, ID/@Rn+1 B FROM (SELECT @I*@N+I-1 ID FROM dbo.Nums) a ) b WHERE ID < @T; 
 SELECT @I = @I+1
END 
ALTER INDEX ALL ON dbo.A5 REBUILD WITH(SORT_IN_TEMPDB=ON, MAXDOP=1, FILLFACTOR=100)
END 
GO

CREATE STATISTICS ST_AID ON dbo.A5(AID) WITH FULLSCAN
CREATE STATISTICS ST_SID ON dbo.A5([SID]) WITH FULLSCAN

SET NOCOUNT ON; 
DECLARE @I int = 0, @N int = 10574, @Card int = 16, @Rn int = 103424, @T int = 1654784; 
DECLARE @R1 int=2, @R2 int=4, @R3 int=8, @R4 int=16, @R5 int=32, @R6 int=64, @R7 int=128, @R8 int=256, @R9 int=512, @RA int=1024, @RY int=250
IF NOT EXISTS(SELECT * FROM dbo.A6)
BEGIN
WHILE @I*@N < @T
BEGIN
 INSERT dbo.A6
 SELECT ID+1 ID, G, S, B, (S-1)%@R1+1 C, (S-1)%@R2+1 D, (S-1)%@R3+1 E
 , ID%@R1+1 R1, ID%@R2+1 R2, ID%@R3+1 R3, ID%@R4+1 R4, ID%@R5+1 R5, ID%@R6+1 R6, ID%@R7+1 R7, ID%@R8+1 R8, ID%@R9+1 R9, ID%@RA+1 RA, ID%@RY+1 GT
 FROM (SELECT ID ID, ID%@Rn+1 G, ID/@Rn+1 S, ID/@Rn+1 B FROM (SELECT @I*@N+I-1 ID FROM dbo.Nums) a ) b WHERE ID < @T; 
 SELECT @I = @I+1
END 
ALTER INDEX ALL ON dbo.A6 REBUILD WITH(SORT_IN_TEMPDB=ON, MAXDOP=1, FILLFACTOR=100)
END 
GO

CREATE STATISTICS ST_AID ON dbo.A6(AID) WITH FULLSCAN
CREATE STATISTICS ST_SID ON dbo.A6([SID]) WITH FULLSCAN