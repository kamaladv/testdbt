{{ config(materialized='view') }}
select 
  ee.entry_id effort_tracking_entry_id,
  ee.protocol_id,
  stage.description effort_tracking_stage,
  category.description effort_tracking_category,
  task.description effort_tracking_task,
  ee.effort_date effort_tracking_date,
  ee.contact_id,
  ee.effort_minutes,
  round((ee.effort_minutes / 60),2) effort_hours,
  ee.comments effort_tracking_comments,
  ee.created_date effort_tracking_created_date,
  ee.created_user effort_tracking_created_username,
  ee.modified_date effort_tracking_modified_date,
  ee.modified_user effort_tracking_modified_username
from 
  {{ source('oncore_src','onc_pcl_effort_entry') }} ee 
join
  {{ source('oncore_src','pf_code') }} stage on ee.stage_code = stage.code_id and stage.category = 'PROTOCOL_STAGE'
join
  {{ source('oncore_src','pf_code') }} category on ee.category_code = category.code_id and category.category = 'EFFORT_CATEGORY'
join
  {{ source('oncore_src','pf_code') }} task on ee.task_code = task.code_id and task.category = 'EFFORT_TASK'
where
  ee._fivetran_deleted = false
  and stage._fivetran_deleted = false
  and category._fivetran_deleted = false
  and task._fivetran_deleted = false