--shipping_country_rates
insert into public.shipping_country_rates (shipping_country, shipping_country_base_rate)
select distinct on (shipping_country, shipping_country_base_rate)
	shipping_country
	, shipping_country_base_rate
from public.shipping s 
;

--shipping_agreement
insert into public.shipping_agreement 
select distinct on (agreementid)
	(regexp_split_to_array(s.vendor_agreement_description, ':'))[1]::int4 as agreementid
	, (regexp_split_to_array(s.vendor_agreement_description, ':'))[2] as agreement_number
	, (regexp_split_to_array(s.vendor_agreement_description, ':'))[3]::numeric(14, 3) as agreement_rate
	, (regexp_split_to_array(s.vendor_agreement_description, ':'))[4]::numeric(14, 3) as agreement_commission
from public.shipping s
;

--shipping_transfer
insert into public.shipping_transfer (transfer_type, transfer_model, shipping_transfer_rate)
select distinct on (transfer_type, transfer_model, shipping_transfer_rate)
	(regexp_split_to_array(s.shipping_transfer_description , ':'))[1] as transfer_type
	, (regexp_split_to_array(s.shipping_transfer_description, ':'))[2] as transfer_model
	, s.shipping_transfer_rate
from public.shipping s
;

--shipping_info
insert into public.shipping_info
select distinct on (shippingid)
	s.shippingid
	, s.vendorid 
	, s.payment_amount 
	, s.shipping_plan_datetime
	, s.shipping_plan_datetime + concat(s.hours_to_plan_shipping, ' hours')::interval as shipping_end_plan_datetime 
	, st.transfer_type_id
	, scr.shipping_country_id
	, (regexp_split_to_array(s.vendor_agreement_description, ':'))[1]::int4 as agreementid --sa.agreementid
from public.shipping s
	left join public.shipping_transfer st on st.transfer_type = (regexp_split_to_array(s.shipping_transfer_description , ':'))[1]
		and st.transfer_model = (regexp_split_to_array(s.shipping_transfer_description, ':'))[2]
	left join public.shipping_country_rates scr on scr.shipping_country = s.shipping_country
--	left join public.shipping_agreement sa on sa.agreementid = (regexp_split_to_array(s.vendor_agreement_description, ':'))[1]::int4
;

--shipping_status
with status_state as (
	select 
		s.shippingid 
		, s.status 
		, s.state 
		, row_number() over (partition by s.shippingid order by s.state_datetime desc) as rn
	from public.shipping s 
)
, shipping_start_fact_datetime as (
	select distinct on (s.shippingid)
		s.shippingid 
		, s.state_datetime as shipping_start_fact_datetime
	from public.shipping s 
	where s.state = 'booked'
)
, shipping_end_fact_datetime as (
	select distinct on (s.shippingid)
		s.shippingid 
		, s.state_datetime as shipping_end_fact_datetime
	from public.shipping s 
	where s.state = 'recieved'
)
, done_table as (
	select
		ss.shippingid
		, ss.status
		, ss.state
		, sfd.shipping_start_fact_datetime
		, sed.shipping_end_fact_datetime
	from status_state as ss
		left join shipping_start_fact_datetime as sfd on sfd.shippingid = ss.shippingid
		left join shipping_end_fact_datetime as sed on sed.shippingid = ss.shippingid
	where ss.rn = 1
)
insert into public.shipping_status
select * from done_table
;
