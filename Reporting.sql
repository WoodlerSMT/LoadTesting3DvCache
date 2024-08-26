DBCC FREEPROCCACHE

/*procedure cache*/
SELECT OBJECT_NAME(DEPS.object_id) Proc_name,
		DEPS.last_logical_reads ,
		DEPS.last_logical_reads * 8 / 1024. ReadsMB ,
		DEPS.last_execution_time,
		DEPS.last_worker_time last_worker_time_us,
		DEPS.max_worker_time max_worker_time_us,
		DEPS.last_elapsed_time  last_elapsed_time_us,
		DEPS.total_worker_time /DEPS.execution_count avg_worker_time_us,
		DEPS.total_elapsed_time /DEPS.execution_count avg_elapsed_time_us,
		DEPS.execution_count
FROM sys.dm_exec_procedure_stats AS DEPS
WHERE DEPS.object_id IN (SELECT object_id FROM sys.procedures AS P WHERE name LIKE '%Test%Proc%')
GO
/*LoadTest Results */
SELECT R.TestRun
		,R.ProcName
		,AVG(R.CPU_Time_us) /1000. Avg_CPU_Time_ms
		,AVG(R.ElapsedTime_us) /1000. Avg_ElapsedTime_ms
		,AVG(R.Res_Wtime_us) /1000. Avg_Res_Wtime_ms
		,AVG(R.Signal_WTime_us) /1000. Avg_Signal_WTime_ms
		,Count(*) Executions
FROM SMT_Collector.LoadTests.ProctestRuns AS R
GROUP BY R.TestRun
		,R.ProcName

SELECT R.TestRun
		,R.ProcName
		,SUM(R.CPU_Time_us) /1000. Total_CPU_Time_ms
		,SUM(R.ElapsedTime_us) /1000. Total_ElapsedTime_ms
		,SUM(R.Res_Wtime_us) /1000. Total_Res_Wtime_ms
		,SUM(R.Signal_WTime_us) /1000. Total_Signal_WTime_ms
		,Count(*) Executions
FROM SMT_Collector.LoadTests.ProctestRuns AS R
GROUP BY R.TestRun
		,R.ProcName


SELECT R.TestRun
		,R.ProcName
		,MAX(R.CPU_Time_us) /1000. Max_CPU_Time_ms
		,MAX(R.ElapsedTime_us) /1000. Max_ElapsedTime_ms
		,MAX(R.Res_Wtime_us) /1000. Max_Res_Wtime_ms
		,MAX(R.Signal_WTime_us) /1000. Max_Signal_WTime_ms
		,Count(*) Executions
FROM SMT_Collector.LoadTests.ProctestRuns AS R
GROUP BY R.TestRun
		,R.ProcName

/*Complete data with relation to waits */
SELECT * FROM SMT_Collector.LoadTests.ProctestRuns ORDER BY ID 
SELECT * FROM SMT_Collector.LoadTests.ProctestRuns_Waits ORDER BY ID, Wait_Time_ms DESC


SELECT TOP 50 * FROM SMT_Collector.LoadTests.ProctestRuns_Waits  ORDER BY ID DESC

/*
TRUNCATE TABLE SMT_Collector.LoadTests.ProctestRuns
TRUNCATE TABLE SMT_Collector.LoadTests.ProctestRuns_Waits
*/

GO

/*WAits stats per Test*/
 SELECT w.wait_type
		,AVG(w.waiting_tasks_count) Avg_waiting_tasks_count
		,MAX(w.waiting_tasks_count) Max_waiting_tasks_count
		,SUM(w.waiting_tasks_count) Total_waiting_tasks_count
		,AVG(w.wait_time_ms) Avg_wait_time_ms
		,MAX(w.wait_time_ms) Max_wait_time_ms
		,SUM(w.wait_time_ms) Total_wait_time_ms
		,AVG(w.signal_wait_time_ms) Avg_signal_wait_time_ms
		,MAX(w.signal_wait_time_ms) Max_signal_wait_time_ms
		,SUM(w.signal_wait_time_ms) Total_signal_wait_time_ms 	
FROM SMT_Collector.LoadTests.ProctestRuns_Waits AS w
WHERE ID IN (SELECT ID  FROM SMT_Collector.LoadTests.ProctestRuns WHERE  TestRun ='TestRunLatchTest')
GROUP BY w.wait_type



SELECT 
	qs.last_execution_time,
    qs.execution_count,
	qs.total_elapsed_time /qs.execution_count avg_elapsed_time,
	qs.total_worker_time/qs.execution_count avg_worker_time,
    qs.total_elapsed_time,
	qs.total_worker_time,
    (qs.total_logical_reads *8) /1024 LogReads_MB,
    st.text AS query_text
FROM 
    sys.dm_exec_query_stats AS qs
CROSS APPLY 
    sys.dm_exec_sql_text(qs.sql_handle) AS st
WHERE 
    st.text LIKE '%FROM Test%MB%'
	
ORDER BY 
    qs.last_execution_time DESC


SELECT * FROM vwPartition WHERE row_cnt >10 AND sch  = 'dbo' AND tabl NOT LIKE '#checks%'
--SELECT * FROM vcol WHERE object_id IN (SELECT b.object_id FROM vwPartition AS b WHERE b.row_cnt >10 AND b.sch  = 'dbo' AND b.tabl NOT LIKE '#checks%')
--SELECT * FROM vtest WHERE row_count > 0
--SELECT * FROM veqs 
--SELECT * FROM vOsBuf WHERE row_count > 0
--SELECT * FROM vDbPtSt

/*
 Table  MB		pages	 rows			rang
 A6		128		16384	1,654,784		 103424
 A5 	386		49152	4,964,352		 310272
 A4		1536	196608	19,857,408		1241088

 SELECT * FROM vwPartition WHERE row_cnt >10 AND sch  = 'dbo' AND tabl NOT LIKE '#checks%'
*/
SELECT '' code,
			query_hash,
			statement_text, execution_count, av_w, 		
			total_elapsed_time/execution_count avg_elapsed_time,
		total_worker_time, total_elapsed_time,	total_worker_time/execution_count avg_worker_time, tphys_reads, tlog_reads, total_rows, start_off, end_off,  creation_time, last_execution_time 
FROM dbo.veqs 
--WHERE  total_worker_time > 100000 AND execution_count >= 1000 
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

SELECT * FROM SMT_Collector.LoadTests.[QueryStatsPerTestRun]