WITH base1 as (
	SELECT id, name
	FROM Students
),
base2 as (
	SELECT
	id,
	name 
	from base1
)
SELECT
id,
name
INTO #temp
from base2