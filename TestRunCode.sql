
DECLARE
/*test Run Name*/
@TestRunName VARCHAR(256) ='DemotestRun3'

/*Running the tests*/ 
--SET STATISTICS IO,TIME ON 
DBCC FREEPROCCACHE
SET NOCOUNT ON 

/*Option to run specific amount of time*/
/*
DECLARE @StartTime DATETIME2 = SYSDATETIME()

WHILE DATEDIFF( MILLISECOND ,@StartTime ,SYSDATETIME())  < 900000 /*900000 = 15 minutes*/
BEGIN
	EXEC dbo.Test28MB_Proc 'DemotestRun1';
END
*/

/**Test 1.A, 1.D**/
/*Fixed execution count*/
/*0x8A64F3203A0610B5*/
EXEC dbo.Test28MB_Proc 'DemotestRun3'
GO 10000
/*0xACA728938721279B*/
EXEC dbo.Test56MB_Proc 'DemotestRun3'
GO 10000-- 10000
/*0x7A67FFB2AC04872C*/
EXEC dbo.Test224MB_Proc 'DemotestRun3'
GO 10000-- 10000
/*0x4C1A67012613D96D*/
EXEC dbo.Test448MB_Proc 'DemotestRun3'
GO 10000-- 10000

/*For fix execution count we will use SQLQueryStress or GO x executions */


/**Test 1.C **/

--DECLARE @TestRunName VARCHAR(256) ='DemotestRun1' /*test Run Name*/
-- Single core test  

DECLARE @Before AS TABLE ( 
						[session_id] [smallint] NOT NULL, 
						[wait_type] [nvarchar](256) NOT NULL, 
						[waiting_tasks_count] [bigint] NOT NULL, 
						[wait_time_ms] [bigint] NOT NULL, 
						[max_wait_time_ms] [bigint] NOT NULL, 
						[signal_wait_time_ms] [bigint] NOT NULL 
						)  

DECLARE @After AS TABLE ( 
						[session_id] [SMALLINT] NOT NULL, 
						[wait_type] [NVARCHAR](256) NOT NULL, 
						[waiting_tasks_count] [BIGINT] NOT NULL, 
						[wait_time_ms] [BIGINT] NOT NULL, 
						[max_wait_time_ms] [BIGINT] NOT NULL, 
						[signal_wait_time_ms] [BIGINT] NOT NULL
						)  
 
INSERT @Before 
SELECT *  
FROM sys.dm_exec_session_wait_stats WHERE session_id = @@SPID 


/* Actual Workload*/ 
DECLARE @T DATETIME, @F BIGINT = 0, @P BIGINT; 
 
SET @T = GETDATE(); 
SET @T = DATEADD(SECOND,60,@T)  /*One Minute Test*/
WHILE @T>GETDATE() 
BEGIN 
SET @P = POWER(2,30) 
SET @F=@F+1 
END 
SELECT @F AS Result INTO #Score 

 
INSERT @After 
SELECT *  
FROM sys.dm_exec_session_wait_stats WHERE session_id = @@SPID 

SELECT COALESCE (a.wait_type , b.wait_type) AS Wait_type 
, COALESCE(a.waiting_tasks_count, b.waiting_tasks_count) - ISNULL(b.waiting_tasks_count, 0)  AS waiting_tasks_count 
, COALESCE(a.wait_time_ms, b.wait_time_ms) - ISNULL(b.wait_time_ms, 0)  AS wait_time_ms 
, COALESCE(a.max_wait_time_ms, b.max_wait_time_ms)  AS max_wait_time_ms 
,'Diff: ' +CAST(COALESCE(a.max_wait_time_ms, b.max_wait_time_ms) - ISNULL(b.max_wait_time_ms, 0)AS NVARCHAR(256))  
+' Aft:' + CAST(a.max_wait_time_ms AS NVARCHAR(256)) 
+' Bfr:'+ CAST(b.max_wait_time_ms AS NVARCHAR(256)) max_wait_diff_m_descr 
, COALESCE(a.signal_wait_time_ms, b.signal_wait_time_ms) - ISNULL(b.signal_wait_time_ms, 0)  AS signal_wait_time_ms 
INTO #Waits
FROM @Before  AS b   
RIGHT JOIN  @After AS a 
ON a.wait_type = b.wait_type 

 INSERT SMT_Collector.LoadTests.[CPUBenchWaits]
 SELECT @TestRunName AS TestRun, *
 FROM #Waits AS W

 INSERT SMT_Collector.LoadTests.[CPUBench]
 SELECT @TestRunName AS TestRun, *
 FROM #Score AS S



/**Test 1.D **/
/* Further tests
Authored by Joe Chang 
https://www.linkedin.com/in/joe-chang-174a451/
*/

/*
 Table  MB		pages	 rows			rang
 A6		128		16384	1,654,784		 103424
 A5 	386		49152	4,964,352		 310272
 A4		1536	196608	19,857,408		1241088

 SELECT * FROM vwPartition WHERE row_cnt >10 AND sch  = 'dbo' AND tabl NOT LIKE '#checks%'
*/


GO
--0xCC18E089BC035491 --A4
DECLARE @I INT, @R INT SELECT @I = 1+1241088*RAND(CHECKSUM(NEWID())); SELECT @R=MAX(b.TI) FROM dbo.A4  a INNER LOOP JOIN dbo.A b ON b.AID = a.AID WHERE a.GID = @I OPTION(MAXDOP 1)
GO 10000
--0xF550DF6E596250B8 --A5
DECLARE @I INT, @R INT SELECT @I = 1+310272*RAND(CHECKSUM(NEWID())); SELECT @R=MAX(b.TI) FROM dbo.A5  a INNER LOOP JOIN dbo.A b ON b.AID = a.AID WHERE a.GID = @I OPTION(MAXDOP 1)
GO 10000
--0xE36ADC079C2D052C -- A6
DECLARE @I INT, @R INT SELECT @I = 1+103424*RAND(CHECKSUM(NEWID())); SELECT @R=MAX(b.TI) FROM dbo.A6  a INNER LOOP JOIN dbo.A b ON b.AID = a.AID WHERE a.GID = @I OPTION(MAXDOP 1)
GO 10000


 /**Test 2.A Parallelism Test **/
 GO
 DECLARE
/*test Run Name*/
@TestRunName VARCHAR(256) ='DemotestRun3', 
@MAXDOP SMALLINT = 8 
/*Configure MAXDOP for the Parallel Query test, should be inside NUMA segment*/
 EXEC Test448MB_Proc_Multi @TestRunName , @MAXDOP
 GO 10000

DECLARE/*test Run Name*/ @TestRunName VARCHAR(256) ='DemotestRun3'
INSERT SMT_Collector.LoadTests.[QueryStatsPerTestRun]
 SELECT @TestRunName,
			query_hash,
			statement_text, execution_count, av_w, 		
			total_elapsed_time/execution_count avg_elapsed_time,
		total_worker_time, total_elapsed_time,	total_worker_time/execution_count avg_worker_time, tphys_reads, tlog_reads, total_rows, start_off, end_off,  creation_time, last_execution_time 
FROM dbo.veqs 
WHERE query_hash IN (
		 0x8A64F3203A0610B5 --Test28MB_Proc
		,0xACA728938721279B --Test56MB_Proc
		,0x7A67FFB2AC04872C --Test224MB_Proc
		,0x4C1A67012613D96D --Test448MB_Proc
		,0xD5857348CC31DEF8 --Test448MB_Proc_Multi
		,0xCC18E089BC035491 --A4
		,0xF550DF6E596250B8 --A5
		,0xE36ADC079C2D052C --A6
		)
ORDER BY execution_count DESC

