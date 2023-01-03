-- Bisque Job that updates the wc-regi-dashboard weekly every Monday. 
-- Link to bisque job https://bisque.prd.nyt.net/jobs/5840

WITH total_impressions AS (
SELECT
      DATE_TRUNC(DATE(_pt), WEEK(monday)) + 6 AS week_end, -- last week data
      COUNT(DISTINCT pg.agent_id) AS imp_count      
  FROM                                                                                                         
      nyt-eventtracker-prd.et.page AS pg,
      unnest(impressions) AS imp
  WHERE
      DATE_TRUNC(DATE(_pt), WEEK(monday)) = {{1 week ago|monday|str}}
      
      AND source_app LIKE '%wirecutter%'
      AND module.name = 'lireInvokedVia' -- name for the regiwall module | cath all log in registration page for all NYT domains
  GROUP BY 1
  ORDER BY 1
),
meter_impressions AS (
SELECT
      DATE_TRUNC(DATE(_pt), WEEK(monday)) + 6 AS week_end, -- last week data
      COUNT(DISTINCT pg.agent_id) AS meter_count
  FROM                                                                                                         
      nyt-eventtracker-prd.et.page AS pg,
      unnest(impressions) AS imp
  WHERE
      DATE_TRUNC(DATE(_pt), WEEK(monday)) = {{1 week ago|monday|str}}
      
      AND source_app LIKE '%wirecutter%'
      AND module.name = 'lireInvokedVia' -- name for the regiwall module | cath all log in registration page for all NYT domains
      AND module.label = 'meter'
  GROUP BY 1
  ORDER BY 1
), 
total_regis AS (
 SELECT
      DATE_TRUNC(DATE(DATETIME(create_date, 'America/New_York')), WEEK(monday)) + 6 AS week_end,
      COUNT(DISTINCT regi.agent_id) AS successful_registrations
  FROM nytdata.auth.regi_tracking AS regi
  WHERE
      DATE_TRUNC(DATE(DATETIME(create_date, 'America/New_York')), WEEK(monday)) = {{1 week ago|monday|str}}
    
      AND client_id = 'wirecutter'
  GROUP BY 1
  ORDER BY 1
 ), 
regiwall_regis AS (
SELECT    
       DATE_TRUNC(DATE(DATETIME(create_date, 'America/New_York')), WEEK(monday)) + 6 AS week_end,
       COUNT(DISTINCT regi.agent_id) AS successful_meter_registrations
    FROM nytdata.auth.regi_tracking AS regi
    WHERE
      DATE_TRUNC(DATE(DATETIME(create_date, 'America/New_York')), WEEK(monday)) = {{1 week ago|monday|str}}
    
      AND client_id = 'wirecutter'
      AND regi.agent_id IN (
               SELECT              --subquery with a list of agent_ids of the people who had a regiwall impression from the meter 
                   pg.agent_id
               FROM                                                                                                     
                   nyt-eventtracker-prd.et.page AS pg,
               unnest(impressions) AS imp
               WHERE
                   DATE_TRUNC(DATE(_pt), WEEK(monday)) = {{1 week ago|monday|str}}
                   
                   AND source_app LIKE '%wirecutter%'
                   AND module.name = 'lireInvokedVia' -- name for the regiwall module | cath all log in registration page for all NYT domains
                   AND module.label = 'meter'
      )
    GROUP BY 1
    ORDER BY 1
), 
regiwall_logins AS (
SELECT
       DATE_TRUNC(DATE(_pt), WEEK(monday)) + 6 AS week_end,
       COUNT(DISTINCT pg.agent_id) AS unique_logins
   FROM
       nyt-eventtracker-prd.et.page AS pg,
       unnest(interactions) AS int
   WHERE
       DATE_TRUNC(DATE(_pt), WEEK(monday)) = {{1 week ago|monday|str}}
       
       AND source_app = 'nyt-lire'
       AND module.label = 'client_id:wirecutter'
       AND LOWER(module.element.name) IN ('login success', 'linked success')
   GROUP BY 1

),
total_anon_users AS (
SELECT 
    DATE_TRUNC(DATE(_pt), WEEK(monday)) + 6 AS week_end,
    COUNT(DISTINCT agent_id) AS total_user_count

    FROM nyt-eventtracker-prd.et.page AS et
    WHERE source_app LIKE '%wirecutter%' 
        AND combined_regi_id is NULL -- filtering for anon users 
        AND DATE_TRUNC(DATE(_pt), WEEK(monday)) = {{1 week ago|monday|str}}    
    GROUP BY 1
    ORDER BY 1
),
anon_usertype AS (
SELECT 
    DATE_TRUNC(DATE(_pt), WEEK(monday)) + 6 AS week_end,

    COUNT(DISTINCT CASE 
            WHEN et.source_app = 'amp-wirecutter' THEN agent_id  
        END) AS anon_amp,
    COUNT(DISTINCT CASE 
            WHEN et.source_app = 'wirecutter' THEN agent_id 
        END) AS non_amp
    FROM nyt-eventtracker-prd.et.page AS et
    WHERE source_app LIKE '%wirecutter%' 
        AND combined_regi_id is NULL -- filtering for anon users 
        AND DATE_TRUNC(DATE(_pt), WEEK(monday)) = {{1 week ago|monday|str}}
    GROUP BY 1
    ORDER BY 1
)
SELECT 
    total_impressions.week_end,
    total_anon_users.total_user_count AS total_anon_users, 
    anon_usertype.anon_amp AS anon_amp_count,
    anon_usertype.non_amp AS anon_non_amp_count,
    total_regis.successful_registrations AS total_regis,
    regiwall_regis.successful_meter_registrations AS regiwall_regis,
    ROUND((regiwall_regis.successful_meter_registrations / meter_impressions.meter_count) * 100, 2) AS CVR,
    ROUND((total_regis.successful_registrations / anon_usertype.non_amp) * 100, 2) AS anon_cvr,
    total_regis.successful_registrations - regiwall_regis.successful_meter_registrations AS other_regis,
    total_impressions.imp_count AS total_impressions,
    meter_impressions.meter_count AS meter_imp_count,
    regiwall_logins.unique_logins AS log_ins,
    meter_impressions.meter_count - regiwall_logins.unique_logins - regiwall_regis.successful_meter_registrations AS bounces_from_rw, 
    ROUND(((meter_impressions.meter_count - regiwall_logins.unique_logins - regiwall_regis.successful_meter_registrations) / meter_impressions.meter_count) * 100,0) AS bounce_rate 

FROM total_impressions
    LEFT JOIN meter_impressions ON meter_impressions.week_end = total_impressions.week_end 
    LEFT JOIN total_regis ON total_regis.week_end = total_impressions.week_end 
    LEFT JOIN regiwall_logins ON regiwall_logins.week_end = total_impressions.week_end 
    LEFT JOIN total_anon_users ON total_anon_users.week_end = total_impressions.week_end 
    LEFT JOIN regiwall_regis ON regiwall_regis.week_end = total_impressions.week_end
    LEFT JOIN anon_usertype ON anon_usertype.week_end = total_impressions.week_end 
