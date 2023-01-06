create or replace table `nyt-bigquery-beta-workspace.aw_data.mbr_reporting_metrics` 
partition by month_year
as 
(

-- 1. Search CTR on Reviews
-- Search is based on session channel
with search_ctr as (
  select 
    year
    , month
    , search_review_ctr
    , sum(num_pclicks) over (partition by year order by year, month)/sum(num_pv) over (partition by year order by year, month) as ytd_search_review_ctr
 from (
      select 
        extract(year from date) as year
        , extract(month from date) as month
        , sum(num_pclicks)/sum(num_pv) as search_review_ctr
        , sum(num_pclicks) as num_pclicks
        , sum(num_pv) as num_pv
      from `nyt-bigquery-beta-workspace.aw_data.ctr_deep_dive_tool` 
      where 1=1
      and session_channel_1 = 'Search'
      and content_type_1 = 'Review'
      and date_trunc(date, month) = date_sub(date_trunc(current_date(), month), interval 1 month) --want the previous month's data
      group by 1, 2
 )
),



-- 2. WAU
weekly_wau as (
  select 
  fiscal_year
  , fiscal_month
  , fiscal_week
  , count(distinct user_id) as num_wau
from `nyt-bigquery-beta-workspace.wirecutter_data.channel` c
left join `nyt-bigquery-beta-workspace.reference.time_dimension` as dim on date(dim.fiscal_week_end_date) = DATE_TRUNC(c.date, WEEK(monday)) + 6 
  where 1=1
    and date_trunc(c.date, month) = date_sub(date_trunc(current_date(), month), interval 1 month) --want the previous month's data
  group by 1, 2, 3
),


-- convert weekly -> monthly
-- monthly wau = avg of weekly wau
monthly_wau as (
  select distinct
    fiscal_year as year
    , fiscal_month as month
    , avg(num_wau) over (partition by fiscal_year, fiscal_month) as monthly_wau
    , avg(num_wau) over (partition by fiscal_year order by fiscal_month) as ytd_wau
  from weekly_wau
),



-- 3. Logged In Users = WAU - Anons
-- some discrepancies exist between this table and `weekly_wau` when calculating total WAU but the % difference is <0.1%. 
weekly_logged_in_users as (
  select
    fiscal_year
  , fiscal_month
  , fiscal_week
  , count(distinct case when wc_user_type <> 'anon' then user_id else null end) as num_logged_in
  , count(distinct case when wc_user_type = 'anon' then user_id else null end) as num_anon
FROM `nyt-bigquery-beta-workspace.ariel_data.wc_readers_detail` r
left join `nyt-bigquery-beta-workspace.reference.time_dimension` as dim on date(dim.fiscal_week_end_date) = DATE_TRUNC(r.date, WEEK(monday)) + 6 
WHERE 1=1
  and date_trunc(r.date, month) = date_sub(date_trunc(current_date(), month), interval 1 month) --want the previous month's data
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
),


monthly_logged_in_users as (
  select
    fiscal_year as year
    , fiscal_month as month
    , avg(num_logged_in) over (partition by fiscal_year, fiscal_month) as monthly_logged_in
    , avg(num_logged_in) over (partition by fiscal_year order by fiscal_month) as ytd_logged_in
  from weekly_logged_in_users
),


-- 4. & 5. Core Affiliate Revenue & EPC
epc as (
  select 
    year
    , month
    , total_aff_earnings
    , monthly_epc
    , sum(total_aff_earnings) over (partition by year order by year, month) as ytd_aff_earnings
    , sum(total_aff_earnings) over (partition by year order by year, month)/sum(num_pclicks) over (partition by year order by year, month) as ytd_epc
from (
      select
        extract(year from date) as year
        , extract(month from date) as month
        , SUM(affiliate_earnings) as total_aff_earnings
        , sum(product_clicks) as num_pclicks
        , SUM(affiliate_earnings)/sum(product_clicks) as monthly_epc
      from `nyt-wccomposer-prd.wc_data_reporting.page_performance_mv`
      where 1=1
        and date_trunc(date, month) = date_sub(date_trunc(current_date(), month), interval 1 month) --want the previous month's data
      group by 1, 2
      )
),



-- 6. WC Only Sub Net Adds
wc_only_daily_snapshot AS 
(
  SELECT
   snapshot_date
   , extract(year from snapshot_date) as year
   , extract(month from snapshot_date) as month
   , sum(subscriber_accounts) as subscriber_accounts --domestic + international
  FROM
    `nyt-bizint-prd.enterprise_sensitive.SA_actives_sub_type`
  WHERE
    financial_entitlement = 'Wirecutter'
    and subscriber_type in ('Wirecutter_Only','Digital_Multi-product')
    and date_trunc(snapshot_date, month) = date_sub(date_trunc(current_date(), month), interval 1 month) --want the previous month's data
  GROUP BY 1, 2, 3
  order by 1, 2, 3
), 



wc_only_net_adds as (
select
  year
  , month
  , net_adds
  , sum(net_adds) over (partition by year order by year, month) as ytd_net_adds
from (
      select distinct
        year
        , month
        , first_value(subscriber_accounts) over (partition by year, month order by snapshot_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_of_month
        , last_value(subscriber_accounts) over (partition by year, month order by snapshot_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_of_month
        , last_value(subscriber_accounts) over (partition by year, month order by snapshot_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) - first_value(subscriber_accounts) over (partition by year, month order by snapshot_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as net_adds
      from wc_only_daily_snapshot
      )
),


-- 7-11. Readers, Pageviews, Sessions, Sessions Per Reader, Pages Per Session
reader_session as (
select
  year
  , month
  , readers
  , pv
  , sessions
  , sessions_per_reader
  , pages_per_session
  -- ytd
  , avg(readers) over (partition by year order by year, month) as ytd_readers --monthly avg
  , sum(pv) over (partition by year order by year, month) as ytd_pv
  , sum(sessions) over (partition by year order by year, month) as ytd_sessions
  , sum(sessions) over (partition by year order by year, month)/sum(readers) over (partition by year order by year, month) as ytd_sessions_per_reader
  , sum(pv) over (partition by year order by year, month)/sum(sessions) over (partition by year order by year, month) as ytd_pages_per_session
from (
      select 
      extract(year from date) as year
      , extract(month from date) as month
      , count(distinct user_id) as readers
      , count(distinct pageview_id) as pv
      , count(distinct concat(user_id, session_index)) as sessions
      , count(distinct concat(user_id, session_index))/count(distinct user_id) as sessions_per_reader
      , count(distinct pageview_id)/count(distinct concat(user_id, session_index)) as pages_per_session
    from `nyt-bigquery-beta-workspace.wirecutter_data.channel` c
      where 1=1
        and date_trunc(date, month) = date_sub(date_trunc(current_date(), month), interval 1 month) --want the previous month's data
      group by 1, 2
    )
),


-- 12 & 13. Total CTR and # Product Clicks
total_ctr as 
(
  select
    year
    , month
    , total_ctr
    , pclicks
    , sum(pclicks) over (partition by year order by year, month)/sum(pv) over (partition by year order by year, month) as ytd_total_ctr
    , sum(pclicks) over (partition by year order by year, month) as ytd_pclicks
from (
    select 
      extract(year from date) as year
      , extract(month from date) as month
      , sum(num_pclicks) as pclicks
      , sum(num_pv) as pv
      , sum(num_pclicks)/sum(num_pv) as total_ctr
    from `nyt-bigquery-beta-workspace.aw_data.ctr_deep_dive_tool` 
    where 1=1
      and date_trunc(date, month) = date_sub(date_trunc(current_date(), month), interval 1 month) --want the previous month's data
    group by 1, 2
    )
),



-- join the above tables
joined_metrics as (
select
  c.year
  , c.month
  --#1
  , c.search_review_ctr
  , c.ytd_search_review_ctr
  --#2
  , w.monthly_wau as wau
  , w.ytd_wau
  -- #3
  , u.monthly_logged_in as logged_in_users
  , u.ytd_logged_in
  -- #4
  , e.total_aff_earnings as aff_revenue
  , e.ytd_aff_earnings
  -- #5
  , e.monthly_epc as epc
  , e.ytd_epc
  -- #6
  , n.net_adds
  , n.ytd_net_adds
  -- #7-11
  , r.* except(year, month)
  -- #12-13
  , tc.* except(year, month)
from search_CTR c
left join monthly_wau w using(year, month)
left join monthly_logged_in_users u using(year, month)
left join epc e using(year, month)
left join wc_only_net_adds n using(year, month)
left join reader_session r using(year, month)
left join total_ctr tc using(year, month)
where 1=1
),

-- select current year
current_year as (
  select distinct
    year
    , month
    , 'actuals' as metric_type
    , search_review_ctr
    , wau
    , logged_in_users
    , aff_revenue 
    , epc
    , net_adds
    , readers
    , pv
    , sessions
    , sessions_per_reader
    , pages_per_session
    , pclicks
    , total_ctr
  from joined_metrics
  where year = extract(year from current_date) -- current year = 2022
),


-- select prior year
prior_year as (
  select distinct
    year + 1 -- +1 to allocate it correctly to 2022
    , month
    , 'prior year' as metric_type
    , search_review_ctr
    , wau
    , logged_in_users
    , aff_revenue 
    , epc
    , net_adds
    , readers
    , pv
    , sessions
    , sessions_per_reader
    , pages_per_session
    , pclicks
    , total_ctr
  from joined_metrics
  where year = extract(year from current_date)-1 -- current year-1 = 2021
),


-- monthly running totals
ytd_totals as (
  select distinct
    year
    , month
    , 'YTD averages' as metric_type
    , ytd_search_review_ctr as search_review_ctr
    , ytd_wau as wau
    , ytd_logged_in as logged_in_users
    , ytd_aff_earnings as aff_earnings
    , ytd_epc as epc
    , ytd_net_adds as net_adds
    , ytd_readers as readers
    , ytd_pv as pv
    , ytd_sessions as sessions
    , ytd_sessions_per_reader as sessions_per_reader
    , ytd_pages_per_session as pages_per_session
    , ytd_pclicks as pclicks
    , ytd_total_ctr as total_ctr
  from joined_metrics
  where year = extract(year from current_date)
),




final_table as (
select * from current_year --where month < extract(month from current_date) -- only retain months that are less than the current months 
union all
select * from prior_year
union all
select * from ytd_totals -- where month < extract(month from current_date)
union all
select * from `nyt-bigquery-beta-workspace.aw_data.mbr_reporting_forecast_figures`
where metric_type is not null
)


select 
  *
  , date(concat(year,"-",month,"-01")) as month_year 
from final_table 
where 1=1
  --and date(concat(year,"-",month,"-01")) < date_trunc(current_date(), month) --only retail months that are less than curent month, if today is 12/5, date trunc is 12/1, and we want values less than that
)
