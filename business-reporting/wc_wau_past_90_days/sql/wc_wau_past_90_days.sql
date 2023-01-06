-- create or replace table  `nyt-bigquery-beta-workspace.aw_data.wc_wau_past_90_days` 
-- partition by week_end
-- as ( 
  
-- by channel
-- For each week, it joins to the previous 12 weeks data (~91 days worth of data) to showcase trends

WITH ut AS
(
SELECT DISTINCT
  CASE
    WHEN financial_entitlements_category = 'Wirecutter' THEN 'WC Only'
    WHEN financial_entitlements_category = 'Multi Products - Single Entitlement' AND financial_entitlement_group LIKE '%Wirecutter%' THEN 'Multi w/ WC'
    WHEN financial_entitlements_category = 'Multi Products - Multi Entitlement'  AND financial_entitlement_group LIKE '%Wirecutter%' THEN 'ADA'
    WHEN financial_entitlements_category = 'HD' AND financial_entitlement_group LIKE '%Wirecutter%' THEN 'HD'
  END AS user_type_a,
  pageview_id,
  dt
FROM `nyt-bizint-prd.enterprise.etsor_sub_sub_mapping`
WHERE 1=1
  and EXTRACT(dayofweek FROM DATE({{yesterday|day|str}})) = 1 --ensures that yesterday is a Sunday
  and date(_PARTITIONTIME) between {{91 days ago|day|str}} and {{yesterday|day|str}} --lookback period is 90 days from yesterday 
  AND financial_entitlement_group LIKE '%Wirecutter%'
)

-- by channel & user_type 
, weekly_subs_by_channel_by_ut as (
SELECT
  date_add(DATE_TRUNC(c.date, week(Monday)), interval 6 day) as week_end,
  user_type_a as wc_user_type,
  session_channel_2,
  APPROX_COUNT_DISTINCT(user_id) AS sub_count,
FROM `nyt-bigquery-beta-workspace.wirecutter_data.channel` c
inner JOIN ut
ON
  ut.pageview_id = c.pageview_id
  AND ut.dt = c.date
where 1=1
  and EXTRACT(dayofweek FROM DATE({{yesterday|day|str}})) = 1 --ensures that yesterday is a Monday
  and date between {{91 days ago|day|str}} and {{yesterday|day|str}} --lookback period is 90 days from yesterday 
GROUP BY 1, 2, 3
)

-- by channel only (& all user types)
, weekly_subs_by_channel_only as (
SELECT
  date_add(DATE_TRUNC(c.date, week(Monday)), interval 6 day) as week_end,
  'All User Types' as wc_user_type,
  session_channel_2,
  APPROX_COUNT_DISTINCT(user_id) AS sub_count,
FROM `nyt-bigquery-beta-workspace.wirecutter_data.channel` c
inner JOIN ut
ON
  ut.pageview_id = c.pageview_id
  AND ut.dt = c.date
GROUP BY 1, 2, 3
)

-- by user type only (& all channels)
, weekly_subs_by_ut_only as (
SELECT
  date_add(DATE_TRUNC(c.date, week(Monday)), interval 6 day) as week_end,
  user_type_a as wc_user_type,
  'All Channels' as session_channel_2,
  APPROX_COUNT_DISTINCT(user_id) AS sub_count,
FROM `nyt-bigquery-beta-workspace.wirecutter_data.channel` c
inner JOIN ut
ON
  ut.pageview_id = c.pageview_id
  AND ut.dt = c.date
GROUP BY 1, 2, 3
)


-- by all channels & all user types
, weekly_subs_by_all as (
SELECT
  date_add(DATE_TRUNC(c.date, week(Monday)), interval 6 day) as week_end,
  'All User Types' as wc_user_type,
  'All Channels' as session_channel_2,
  APPROX_COUNT_DISTINCT(user_id) AS sub_count,
FROM `nyt-bigquery-beta-workspace.wirecutter_data.channel` c
inner JOIN ut
ON
  ut.pageview_id = c.pageview_id
  AND ut.dt = c.date
GROUP BY 1, 2, 3
)


--  combine the above tables
, weekly_view as (
  select * from weekly_subs_by_channel_by_ut
  union all
  select * from weekly_subs_by_channel_only
  union all
  select * from weekly_subs_by_ut_only
  union all
  select * from weekly_subs_by_all
)



, comparison_dates as (
  select
    a.*
  , lag(sub_count, 1) over (partition by session_channel_2, wc_user_type order by week_end) as last_wk_sub_count
  , row_number() over (partition by session_channel_2, wc_user_type order by week_end)-12 as row_start
  , row_number() over (partition by session_channel_2, wc_user_type order by week_end) as row_end
  from weekly_view a

)


, comparison_columns as (
  select
    a.*
  , safe_divide(sub_count, last_wk_sub_count) - 1 as last_wk_percent_change
  , sub_count - last_wk_sub_count as last_wk_change
  , b.week_end as past_90_week_end
  , b.row_num
  , b.moving_sub_count
  from comparison_dates a
  inner join (
    select distinct
      week_end
      , session_channel_2
      , wc_user_type
      , sub_count as moving_sub_count
      , row_number() over (partition by session_channel_2, wc_user_type order by week_end) as row_num
    from weekly_view
  ) as b on 
        b.row_num between a.row_start and a.row_end 
        and b.session_channel_2 = a.session_channel_2
        and b.wc_user_type = a.wc_user_type
)

select * from comparison_columns where week_end = {{yesterday|day|str}}
-- order by wc_user_type, session_channel_2

-- )
