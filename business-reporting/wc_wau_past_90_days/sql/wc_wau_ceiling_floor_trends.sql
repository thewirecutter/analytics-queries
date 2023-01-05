--------

-- create or replace table  `nyt-bigquery-beta-workspace.aw_data.wc_wau_ceiling_floor_trends_temp` 
-- partition by week_end
-- as (

with channel_ranges as (
  select distinct
    *except(row_num, moving_sub_count, row_end, row_start)
    , percentile_cont(moving_sub_count, 0.25) over (partition by session_channel_2, wc_user_type, week_end) as past_90_floor
    , percentile_cont(moving_sub_count, 0.75) over (partition by session_channel_2, wc_user_type, week_end) as past_90_ceiling
    , percentile_cont(moving_sub_count, 0.75) over (partition by session_channel_2, wc_user_type, week_end) - percentile_cont(moving_sub_count, 0.25) over (partition by session_channel_2, wc_user_type, week_end) as IQR
    , percentile_cont(moving_sub_count, 0.25) over (partition by session_channel_2, wc_user_type, week_end) - 1.5*(percentile_cont(moving_sub_count, 0.75) over (partition by session_channel_2, wc_user_type, week_end) - percentile_cont(moving_sub_count, 0.25) over (partition by session_channel_2, wc_user_type, week_end)) as past_90_lower_outlier_range
    , percentile_cont(moving_sub_count, 0.75) over (partition by session_channel_2, wc_user_type, week_end) + 1.5*(percentile_cont(moving_sub_count, 0.75) over (partition by session_channel_2, wc_user_type, week_end) - percentile_cont(moving_sub_count, 0.25) over (partition by session_channel_2,wc_user_type, week_end)) as past_90_upper_outlier_range
  from `nyt-bigquery-beta-workspace.aw_data.wc_wau_past_90_days`
  where week_end = {{yesterday|day|str}}
)



, final_table as (
  select distinct
    *
    , case
      when sub_count >= past_90_upper_outlier_range then 'Outlier +'
      when sub_count <= past_90_lower_outlier_range then 'Outlier -'
      when sub_count > past_90_ceiling then 'Above Ceiling'
      when sub_count < past_90_floor then 'Below Floor'
      else 'Normal'
      end as past_90_movement
  from channel_ranges
  where extract(year from week_end) >= 2022 --only keep weeks from 2022-01-03 and beyond
)


select * from final_table
--)
