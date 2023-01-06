declare relative_date date;
set relative_date = '2022-12-26';

-- Initial base table with pageviews by user type and channel
with user_base_table as (
    select
        ut.date
        , date_add(DATE_TRUNC(c.date, week(Monday)), interval 6 day) as week_end
        , user_type_2
        , session_channel_2
        , ut.user_id
    from `nyt-bigquery-beta-workspace.wirecutter_data.user_type` ut
    left join `nyt-bigquery-beta-workspace.wirecutter_data.channel` c using(pageview_id)
    where EXTRACT(dayofweek FROM date_sub(relative_date, interval 1 day)) = 1 --ensures that yesterday is a Monday
  and ut.date between date_sub(relative_date, interval 91 day) and date_sub(relative_date, interval 1 day) --lookback period is 90 days from yesterday 
)

-- disregard user type and session channel
, weekly_subs_by_all as (
SELECT
    week_end
    , 'All User Types' as wc_user_type
    , 'All Channels' as session_channel_2
    , APPROX_COUNT_DISTINCT(user_id) AS sub_count,
FROM user_base_table
GROUP BY 1, 2, 3
)

-- by both channel & user_type 
, weekly_subs_by_channel_by_ut as (
SELECT
    week_end
    , user_type_2 as wc_user_type
    , session_channel_2,
  APPROX_COUNT_DISTINCT(user_id) AS sub_count,
FROM user_base_table
where 1=1
GROUP BY 1, 2, 3
)

-- by channel ONLY (& all user types)
, weekly_subs_by_channel_only as (
SELECT
    week_end
    , 'All User Types' as wc_user_type
    , session_channel_2
    , APPROX_COUNT_DISTINCT(user_id) AS sub_count
FROM user_base_table
GROUP BY 1, 2, 3
)

-- by user type ONLY (& all channels)
, weekly_subs_by_ut_only as (
SELECT
    week_end
    , user_type_2 as wc_user_type
    , 'All Channels' as session_channel_2
    , APPROX_COUNT_DISTINCT(user_id) AS sub_count
FROM user_base_table
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


-- adding columns to compare to the current week: 1) compared to last week and 2) compared between now and past 12 weeks
, comparison_dates as (
  select
    a.*
  , lag(sub_count, 1) over (partition by session_channel_2, wc_user_type order by week_end) as last_wk_sub_count
  , row_number() over (partition by session_channel_2, wc_user_type order by week_end)-12 as row_start
  , row_number() over (partition by session_channel_2, wc_user_type order by week_end) as row_end
  from weekly_view a

)


-- joining the past 12 weeks back onto each week
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


select * from comparison_columns 
-- order by wc_user_type, session_channel_2

-- )






















