{{
    config(
        materialized='ephemeral'
    )
}}

select 
  pd.pcl_detail_id protocol_group_id,
  pd.protocol_id,
  d.category group_type_key,
  case 
    when d.category = 'PROGRAM_AREA' then 'Program Area'
    when d.category = 'DOWG' then coalesce(oconf.value,'DOWG')
  end group_type,
  'Not applicable' management_group_ou_name,
  d.code group_code,
  d.description group_name,
  case 
    when d.category = 'PROGRAM_AREA' then dp.description 
    else 'Not applicable'
  end program_area_status,
  case when pd.value = 'Y' then 'Yes' else 'No' end primary_group_of_group_type,
  pd.created_user group_created_username,
  pd.created_date group_created_date,
  pd.modified_user group_modified_username,
  pd.modified_date group_modified_date
from 
  {{ source('oncore_src','smrs_pcl_detail') }} pd
left join
  {{ source('oncore_src','oncore_configs') }} oconf on oconf.key = 'DOWG' and oconf._fivetran_deleted = false
join 
  {{ source('oncore_src','pf_code') }} d on pd.detail = d.code_id
left join
  {{ source('oncore_src','pf_code') }} dp on d.parent_id = dp.code_id and dp.category = 'PROGRAM_AREA_STATUS'
where 
  pd._fivetran_deleted = false
and oconf._fivetran_deleted = false
and d._fivetran_deleted = false
and dp._fivetran_deleted = false
and
  d.category in ('DOWG','PROGRAM_AREA')
  