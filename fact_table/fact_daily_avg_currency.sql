-- Create fact daily average currency

create table if not exists final_project.fact_daily_avg_currency (
	currency_id varchar,
	currency_name varchar,
	day date,
	avg_rate float
);

insert into final_project.fact_daily_avg_currency (
	currency_id,
	currency_name,
	day,
	avg_rate
)
(
select 
	currency_id,
	currency_name,
	"timestamp"::date as day,
	avg(rate) as avg_rate
from final_project.topic_currency
where (to_timestamp("timestamp",'YYYY-MM-DD HH24:MI:SS'))::date = '{{ execution_date }}'::date - interval '1 day'
group by currency_id , currency_name , "timestamp"::date
order by currency_id
)
;
