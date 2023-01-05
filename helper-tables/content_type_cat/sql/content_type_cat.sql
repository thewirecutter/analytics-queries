create or replace table `nyt-bigquery-beta-workspace.aw_data.content_type_cat` as (
with all_content_types as (
    select 
        distinct 
        REGEXP_EXTRACT(ga_id, '[A-Za-z]+') as cms_id
        , case --manually correct some page types for consistency
            when REGEXP_EXTRACT(ga_id, '[A-Za-z]+') = 'CL' then 'Collective' --otherwise it's listed as both a Review and Collective
            when REGEXP_EXTRACT(ga_id, '[A-Za-z]+') = 'PO' then 'Blog' --otherwise it's listed as "Post"
            else page_type 
            end as content_type_3
    from `wc-ga-167813.wc_data_reporting.wc_all_pages`
    where 1=1
),


content_type_cat_2 as (
    select
        *
        , case
            when content_type_3 in ('Buying Guide','Review','Single','Subjective','Temporary Template') then 'Core Review'
            when content_type_3 in ('Collective','How To','Listicle','Staff Pick') then 'Non-Core Review'
            when content_type_3 in ('Home','Search','Leaderboard','Section','404','All') then 'Site Navigation'
            when content_type_3 in ('Blog','List') then content_type_3
            when content_type_3 = 'Special Event' then 'Deals Page'
            else 'Other'
            end as content_type_2
    from all_content_types
),

content_type_cat_1 as (
    select
        *
        , case
            when content_type_2 in ('Core Review','Non-Core Review') then 'Review'
            else content_type_2
            end as content_type_1
    from content_type_cat_2
)

select * from content_type_cat_1 order by content_type_1, content_type_2
)
