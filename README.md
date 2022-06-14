# Проект 2
# Миграция схемы и создание витрины

## 1 Выяснение требований

Задача:

Создать новую схему и произвести миграцию данных в нее.
Построить витрину данных на основе новой схемы.

**Структура витрины:**

shippingid
vendorid
transfer_type
full_day_at_shipping
is_delay
is_shipping_finish
delay_day_at_shipping
payment_amount
vat
profit

**Глубина:** не оговорено (весь период по данным)

**Частота обновлений:** отсутствует.

**Локация витрины:** public.shipping_datamart

**Доступы:** не обговорены.

**Срок задачи:** отсутствует.

**Метрики:**

full_day_at_shipping — количество полных дней, в течение которых длилась доставка. Высчитывается как:shipping_end_fact_datetime-shipping_start_fact_datetime.

is_delay — статус, показывающий просрочена ли доставка. Высчитывается как:shipping_end_fact_datetime >> shipping_plan_datetime → 1 ; 0

is_shipping_finish — статус, показывающий, что доставка завершена. Если финальный status = finished → 1; 0

delay_day_at_shipping — количество дней, на которые была просрочена доставка. Высчитыается как: shipping_end_fact_datetime >> shipping_end_plan_datetime → shipping_end_fact_datetime -− shipping_plan_datetime ; 0).

payment_amount — сумма платежа пользователя

vat — итоговый налог на доставку. Высчитывается как: payment_amount *∗ ( shipping_country_base_rate ++ agreement_rate ++ shipping_transfer_rate)

profit — итоговый доход компании с доставки. Высчитывается как: payment_amount*∗ agreement_commission.


## 2 Создание бэкапа

pg_dump --username jovyan  --host localhost --port 5432 --dbname de --file project2_initial_dump_20220611.sql
psql --username jovyan --dbname de --file project2_initial_dump_20220611.sql

Скрипт отката не создается, так как никакие откаты не нужны ( в исходных данные ничег оне меняется)


## 3 Изучение схемы и данных

Изучал через dbeaver.
В задании на сайте была приложена схема, которой не оказалось в БД.
Из схемы была только таблица shipping.

-----------
```SQL
-- уникальность перехода состояния в booked
select
	s.shippingid 
	, count(*) as cc
from public.shipping s 
where s.state = 'booked'
group by s.shippingid
having count(*) > 1
;

-- уникальность перехода состояния в received
select
	s.shippingid 
	, count(*) as cc
from public.shipping s 
where s.state = 'recieved'
group by s.shippingid
having count(*) > 1
;
```


## 4 Создание схемы

```SQL
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
```

psql postgresql://jovyan:jovyan@localhost:5432/de < schema_creating.sql


## 4 Миграция данных

Далее вам необходимо создать витрину. Напишите CREATE TABLE запрос и выполните его на предоставленной базе данных в схеме analysis.

```SQL
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
	, s.hours_to_plan_shipping
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
```

psql postgresql://jovyan:jovyan@localhost:5432/de < data_migration.sql


## 5 Проверка правильности заполнения

Посмотрел в dbeaver данные в ручную + есть связи в таблицах по ключам и ограничения.
