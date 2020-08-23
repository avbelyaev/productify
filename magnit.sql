drop table SCHEDULERS;

CREATE TABLE SCHEDULERS (
	ID_NAME VARCHAR(32), 
	NAME VARCHAR(32),
	SCHEDULE VARCHAR(32),
	DATE_BEGIN_STR VARCHAR(32),
	DATE_END_STR VARCHAR(32),
	DATE_BEGIN TIMESTAMP,
	DATE_END TIMESTAMP
);

COPY SCHEDULERS(NAME, SCHEDULE, DATE_BEGIN_STR, DATE_END_STR)
FROM '/tmp/shedulers.csv'
DELIMITER ';'
CSV HEADER;

select * from SCHEDULERS;

update SCHEDULERS
set 
	DATE_BEGIN = TO_TIMESTAMP(DATE_BEGIN_STR, 'DD.MM.YYYY HH24:MI'),
	DATE_END = TO_TIMESTAMP(DATE_END_STR, 'DD.MM.YYYY HH24:MI');

select * from SCHEDULERS;



DROP TABLE IF EXISTS T_CONTRACTOR_WORK_DAY;
CREATE TABLE T_CONTRACTOR_WORK_DAY (
--	ID VARCHAR(32),
	NAME VARCHAR(32),
	DATE_BEGIN TIMESTAMP,
	DATE_END TIMESTAMP
);


CREATE OR REPLACE PROCEDURE PRC_SHEDULERS (
	p_DATE_BEGIN DATE, 
	p_DATE_END DATE
)
language plpgsql
AS $$
BEGIN

drop table days;
create temp table days as
--select generate_series(p_DATE_BEGIN, p_DATE_END, '1 day'::interval);
select generate_series('2019-01-01', '2019-01-10', '1 day'::interval);

select * from days;

DELETE FROM T_CONTRACTOR_WORK_DAY;

drop table full_schedule;
create temp table full_schedule as
SELECT s.NAME
	 , s.SCHEDULE
	 , s.DATE_BEGIN
	 , s.DATE_END
	 , ROW_NUMBER () OVER (PARTITION BY s.NAME, s.SCHEDULE, s.DATE_BEGIN order by generate_series) AS rn
	 , generate_series as date_current
FROM SCHEDULERS AS s
cross join days 
where generate_series between s.DATE_BEGIN and s.DATE_END;

select * from full_schedule;

drop table schedule_by_day;
create temp table schedule_by_day as
SELECT s.NAME
	 , s.SCHEDULE
	 , date_current
	 , s.DATE_BEGIN
	 , s.DATE_END
	 , s.rn
	 , SUBSTR(s.SCHEDULE, COALESCE(NULLIF(s.RN % LENGTH(s.SCHEDULE), 0), LENGTH(s.SCHEDULE))::integer, 1) as schedule_day
FROM full_schedule AS s;

select * from schedule_by_day;

INSERT INTO T_CONTRACTOR_WORK_DAY
SELECT s.NAME
	 , CASE WHEN s.schedule_day = 'д' THEN INTERVAL '8 hour' + CAST (s.date_current AS timestamp(3))
			WHEN s.schedule_day = 'н' THEN INTERVAL '20 hours' + CAST (s.date_current AS timestamp(3))
			WHEN s.schedule_day = 'с' THEN INTERVAL '8 hour' + CAST (s.date_current AS timestamp(3)) END AS DATE_BEGIN

	 , CASE WHEN s.schedule_day = 'д' THEN INTERVAL '20 hours' + CAST (s.date_current AS timestamp(3))
			WHEN s.schedule_day = 'н' THEN INTERVAL '32 hours' + CAST (s.date_current AS timestamp(3))
			WHEN s.schedule_day = 'с' THEN INTERVAL '32 hours' + CAST (s.date_current AS timestamp(3)) END AS DATE_END
FROM schedule_by_day AS s
WHERE s.schedule_day != 'в';

END;$$


CALL PRC_SHEDULERS('2019-01-03', '2019-02-02');

