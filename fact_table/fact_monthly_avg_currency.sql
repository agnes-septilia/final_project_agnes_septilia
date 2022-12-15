-- Create fact monthly average currency

create table if not exists final_project.fact_monthly_avg_currency (
	currency_id varchar,
	currency_name varchar,
	end_of_month date,
	avg_rate float
);

insert into final_project.fact_monthly_avg_currency (
	currency_id,
	currency_name,
	end_of_month,
	avg_rate
)
(
select 
	currency_id,
	currency_name,
	date_trunc('month', to_timestamp("timestamp",'YYYY-MM-DD HH24:MI:SS')) + interval '1 month' - interval '1 day' as end_of_month,
	avg(rate) as avg_rate
from final_project.topic_currency
where date_trunc('month', to_timestamp("timestamp",'YYYY-MM-DD HH24:MI:SS')) = date_trunc('month', '{{ execution_date }}'::date - interval '1 day')
group by currency_id , currency_name , date_trunc('month', to_timestamp("timestamp",'YYYY-MM-DD HH24:MI:SS'))
order by currency_id
)
;
