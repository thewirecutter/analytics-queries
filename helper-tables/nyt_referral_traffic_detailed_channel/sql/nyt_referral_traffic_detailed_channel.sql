-- identify pageviews with session_channel = NYT Referral 

with nyt_ref_pages as (
  select
    date(_pt) as date
    , pg.pageview_id
    , pg.agent_id
    , session_channel_3
    , c.session_index
    , c.agent_day_session_pageview_index
    , first_value(pg.pageview_id) over (partition by pg.agent_id, c.session_index order by agent_day_session_pageview_index) as first_session_pv
    , first_value(url.raw) over (partition by pg.agent_id, c.session_index order by agent_day_session_pageview_index) as first_session_url
  from nyt-eventtracker-prd.et.page AS pg
  inner join (
    select
      date
      , pageview_id
      , session_index
      , agent_day_session_pageview_index
      , session_channel_3
    from `nyt-bigquery-beta-workspace.wirecutter_data.channel`
    where 1=1
      and date between '2022-11-24' and '2022-11-28'
      -- and session_channel_2 = 'NYT Referral'
  ) as c on date(pg._pt) = c.date and pg.pageview_id = c.pageview_id 
where 1=1
  and date(_pt) between '2022-11-24' and '2022-11-28'
  and source_app like '%wirecutter%'
)

-- further categorize beyond NYT Referral
-- add product clicks
, session_channel_breakdown as (
  select
    pg.*
    , case 
      when first_session_url like '%the-morning%'then "The Morning" 
      when first_session_url like '%edit_ufn_%' then 'UFN Emails'
      else session_channel_3 
      end as session_channel_4 --add a more granular channel view
    , num_pclicks
  from nyt_ref_pages pg
  left join (
        SELECT 
            date(_pt) as date
            , pageview_id
            , count(int.module.element.name) as num_pclicks
        FROM
            nyt-eventtracker-prd.et.page AS pg,
            unnest(interactions) AS int
        WHERE
            DATE(_pt) between '2022-11-24' and '2022-11-28'
            AND source_app LIKE '%wirecutter%'
            and int.module.element.name like '%outbound_product%'
        group by 1, 2
        ) as pc on 
        pg.pageview_id = pc.pageview_id
)


, final_table as (
  select
    date
    , session_channel_4
    , count(distinct pageview_id) as num_pv
    , sum(num_pclicks) as num_pclicks
  from session_channel_breakdown
  group by 1, 2
)


select * from final_table order by 1, 2
-- select count(*), count(distinct pageview_id) from session_channel_breakdown