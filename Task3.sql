WITH CTE_1 AS (
	--The names and the number of messages sent by each user
	SELECT
		Task = '1'
		,Col1 = COUNT(msg.MessageID)
		,Col2 = sender.Name
	FROM Messages msg
		LEFT JOIN User sender
			ON msg.UserIDSender = sender.UserID
	Group By sender.UserID
),  CTE_2 AS (
	--The total number of messages sent stratified by weekday
	SELECT
		Task = '2'
		,Col1 = COUNT(msg.MessageID)
		,Col2 = DATE_PART(dayofweek, DateSent)
	FROM Messages msg
	WHERE DATE_PART(dayofweek, DateSent) BETWEEN 1 AND 5
	GROUP BY DATE_PART(dayofweek, DateSent)
), CTE_3 AS (
	--The most recent message from each thread that has no response yet
	SELECT DISTINCT
		Task = '3'
		,Col1 = Msg.MessageContent
		,Col2 = thr.ThreatID
	FROM THREADS thr
		INNER JOIN MESSAGES msg
			ON thr.ThreadID = msg.ThreadID
	WHERE COUNT(MessageID) = 1
), CTE_4 AS (
	--For the conversation with the most messages: all user data and message contents ordered chronologically so one can follow the whole conversation.
		SELECT
			Task = '4'
			,Col1 = ThreadID
			,Col2 = UserIDSender
			,Col3 = UserIDRecipient
			,Col4 = MessageContent
			,Col5 = DateSent
		FROM Messages msg
		WHERE msg.ThreadID in (
							SELECT
								MAX(COUNT(msg.MessageID))
								,thr.ThreadID
							FROM Messages msg
								INNER JOIN Threads thr
									ON msg.ThreadID = thr.ThreadID
							Group by thr.ThreadID
							)
		ORDER BY DateSent DESC
		)
	/** 
	Union all 4 CTEs to combine the result sets into a single query
	**/
		
	SELECT Task, Col1, Col2, Col3='', Col4='', Col5='' FROM CTE_1 
	UNION 
	SELECT Task, Col1, Col2, Col3='', Col4 =''. Col5='' FROM CTE_2
	UNION
	SELECT Task, Col1, Col2, Col3='',Col4='', Col5='' FROM CTE_3
	UNION
SELECT Task, Col1, Col2, Col3, Col4, Col5 FROM CTE_4
