drop view if exists public.shipping_datamart;
drop table if exists public.shipping_status;
drop table if exists public.shipping_info;
drop table if exists public.shipping_country_rates;
drop table if exists public.shipping_agreement;
drop table if exists public.shipping_transfer;

--shipping_country_rates
create table if not exists public.shipping_country_rates (
	shipping_country_id serial not null,
	shipping_country text null,
	shipping_country_base_rate numeric(14, 3) null,
	primary key (shipping_country_id),
	constraint shipping_country_rates_unique unique (shipping_country, shipping_country_base_rate)
);

--shipping_agreement
create table if not exists public.shipping_agreement (
	agreementid int4 not null,
	agreement_number text null,
	agreement_rate numeric(14, 3) null,
	agreement_commission numeric(14, 3) null,
	primary key (agreementid)
);

--shipping_transfer 
create table if not exists public.shipping_transfer (
	transfer_type_id serial not null,
	transfer_type text null,
	transfer_model text null,
	shipping_transfer_rate numeric(14, 3) null,
	primary key (transfer_type_id)
);

--shipping_info 
create table if not exists public.shipping_info (
	shippingid int8 not null,
	vendorid int8 null,
	payment_amount numeric(14, 2) null,
	shipping_plan_datetime timestamp null,
	hours_to_plan_shipping numeric(14, 2) null,
	transfer_type_id int null,
	shipping_country_id int null,
	agreementid int null,
	primary key (shippingid),
	foreign key (transfer_type_id) references public.shipping_transfer (transfer_type_id) on update cascade,
	foreign key (shipping_country_id) references public.shipping_country_rates (shipping_country_id) on update cascade,
	foreign key (agreementid) references public.shipping_agreement (agreementid) on update cascade
);

--shipping_status 
create table if not exists public.shipping_status (
	shippingid int8 not null,
	status text null,
	state text null,
	shipping_start_fact_datetime timestamp null,
	shipping_end_fact_datetime timestamp null,
	primary key (shippingid)
);

--shipping_datamart 
create or replace view public.shipping_datamart as 
select 
	si.shippingid
	, si.vendorid
	, st.transfer_type 
	, extract(day from age(ss.shipping_end_fact_datetime, ss.shipping_start_fact_datetime)) as full_day_at_shipping 
	, case 
		when ss.shipping_end_fact_datetime > si.shipping_plan_datetime then 1 
		else 0
	end as is_delay
	, case
		when ss.status = 'finished' then 1
		else 0
	end as is_shipping_finish 
	, case
		when ss.shipping_end_fact_datetime > (si.shipping_plan_datetime + concat(si.hours_to_plan_shipping, ' hours')::interval)
		then extract(day from age(ss.shipping_end_fact_datetime, si.shipping_plan_datetime))
		else 0
	end as delay_day_at_shipping 
	, si.payment_amount 
	, si.payment_amount * (scr.shipping_country_base_rate + sa.agreement_rate + st2.shipping_transfer_rate) as vat 
	, si.payment_amount * sa.agreement_commission as profit 
from public.shipping_info si 
	left join public.shipping_status ss on ss.shippingid = si.shippingid 
	left join public.shipping_transfer st on st.transfer_type_id = si.transfer_type_id 
	left join public.shipping_country_rates scr on scr.shipping_country_id = si.shipping_country_id 
	left join public.shipping_agreement sa on sa.agreementid = si.agreementid 
	left join public.shipping_transfer st2 on st2.transfer_type_id = si.transfer_type_id 
;
