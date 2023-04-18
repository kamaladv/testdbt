{{
    config(
        materialized='ephemeral'
    )
}}

select
  mgmtgrp.id protocol_group_id,
  mgmtgrp.protocol_id,
  'MANAGEMENT_GROUP' group_type_key,
  'Management Group' group_type,
  ou.name management_group_ou_name,
  dm.code group_code,
  dm.name group_name,
  'Not applicable' program_area_status,
  case when mgmtgrp.primary_flag = 'Y' then 'Yes' else 'No' end primary_group_of_group_type,
  mgmtgrp.created_user group_created_username,
  mgmtgrp.created_date group_created_date,
  mgmtgrp.modified_user group_modified_username,
  mgmtgrp.modified_date group_modified_date
from  
  {{ source('oncore_src','onc_pcl_management_group') }} mgmtgrp
join 
  {{ source('oncore_src','onc_org_unit_management_group') }} rmg on mgmtgrp.research_management_group_id = rmg.onc_ou_mgmt_group_id
join 
  {{ source('oncore_src','onc_organizational_unit') }}  ou on rmg.onc_organizational_unit_id = ou.onc_organizational_unit_id
join 
  {{ source('oncore_src','onc_management_group') }} dm on rmg.onc_management_group_id = dm.onc_management_group_id
where
  mgmtgrp._fivetran_deleted = false
  and rmg._fivetran_deleted = false
  and ou._fivetran_deleted = false
  and dm._fivetran_deleted = false