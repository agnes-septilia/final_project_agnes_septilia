-- Create dim state (id, country_id, state_code)

create table if not exists final_project.dim_state (
	id uuid unique,
	country_id uuid,
	state_code varchar,
	primary key(id),
	foreign key(country_id) references final_project.dim_country(id),
	constraint country_state unique (country_id, state_code)
);

insert into final_project.dim_state (
  id, 
  country_id,
  state_code
)
(
	with combined_data as (
		-- get data from companies
		select distinct
			case when offices_country_code is null or offices_country_code = '' then 'others' else offices_country_code end as country_code,
			case when offices_state_code is null or offices_state_code = '' then 'others' else offices_state_code end as state_code
		from final_project.sample_training_companies
		
		union
		
		-- get data from zips
		select distinct 
			'others' as country_code,
			state as state_code
		from final_project.sample_training_zips
	)
	
	-- impute null values and add uuid generator
	, imputed as (
		select 
			gen_random_uuid() as id, 
			country_code,
			state_code
		from combined_data
	)
	
	-- combine with dim_country to get country uuid
	select
		imputed.id,
		dc.id as country_id,
		imputed.state_code
	from imputed
	left join final_project.dim_country dc 
		on dc.country_code = imputed.country_code
)
on conflict (country_id, state_code) do nothing 
;
