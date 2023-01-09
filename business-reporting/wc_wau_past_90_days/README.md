# wc_wau_by_channel
WC WAU by channel with last 90 days trend

## Purpose
The goal of this repo is to track the # of WC-entitled Weekly Active Users onsite (i.e. subs only) for any given week, by session channel and by user type (aka. entitlement). In addition to the week's WAU counts, there is also a lookback period of 90 days that is used to calculate ceiling/floor/outlier ranges to help isolate if trends are truely meaningful. 


This repo will provide the following metrics:
- **WAU**: for every Channel and User Type combination for the past week
- **Last Week Percent Change**: The % change compared to the previous week
- **Last Week Change**: The # change comepared to the previous week
- **Past 90 Movement**: Based on the previous 90 days' values, we categorize the current week as (Below Floor/Above Ceiling/Normal/Outlier)


## Tables
1. `wc_wau_past_90_days`: For every week, the last 12 weeks are appended on the right so that each week's data is duplicated 13 times. Calculations are also done for each week
2. `wc_wau_ceiling_floor_trends`: Builds upon the above table to calculate the ceiling/floor/outier ranges, which is then used to classify the current week's WAU value. 


## Dashboards
- [Wirecutter Subscriber WAU](https://app.mode.com/nytimes/reports/0e90ff940efe): Mode dashboard to help explain weekly % of Subs Onsite figure


## Refresh ##
1. [Bisque job for wc_wau_past_90_days](https://bisque.prd.nyt.net/jobs/6597): This job will run after the completion of the sub_sub tables, and a new partition will be added every Monday to capture the previous week's data. 
2. [Bisque job for wc_wau_ceiling_floor_trends](https://bisque.prd.nyt.net/jobs/6601): This job will run after the completion of the table above. 
