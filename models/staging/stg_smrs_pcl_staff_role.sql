{{
    config(
        materialized='ephemeral'
    )
}}


select
  sr.protocol_staff_role_id protocol_staff_role_id,
  case 
    when sr.protocol_subject_id is not null then 'Subject'
    else 'Protocol'
  end staff_role_type,  
  sr.protocol_id protocol_id,
  sr.protocol_subject_id protocol_subject_id,
  r.code staff_role_code,
  r.description staff_role_name,   
  sr.contact_id contact_id,   
  sr.organization_id staff_protocol_organization_id,  
  apo.additional_protocol_access_organization,
  sr.start_date protocol_staff_start_date,
  sr.stop_date protocol_staff_stop_date,
  sr.stop_reason protocol_staff_stop_reason,
  case 
    when 
      current_date between coalesce(sr.start_date,current_date) and coalesce(sr.stop_date,current_date) 
    then 'Yes'
  else 'No'
  end active_protocol_staff,
   random_admin.can_verify_randomization_questionnaire,
   random_admin.can_assign_randomized_treatment,
   random_admin.can_manually_assign_treatment,
   random_admin.can_see_blinded_treatment,    
  sr.created_user protocol_staff_created_username,  
  sr.created_date protocol_staff_created_date,
  sr.modified_user protocol_staff_modified_username,
  sr.modified_date protocol_staff_modified_date
from 
  {{ source('oncore_src','smrs_pcl_staff_role') }} sr
join
  {{ source('oncore_src','pf_code') }} r on r.category = 'STAFF_ROLE' and sr.staff_role = r.code_id
left join
  {{ ref('smrs_pcl_staff_role1') }} random_admin on sr.protocol_staff_role_id = random_admin.protocol_staff_role_id
left join
  {{ ref('stg_data_for_addtl_protocol_org') }} apo on sr.protocol_staff_role_id = apo.protocol_staff_role_id
where
   sr._fivetran_deleted = false

   

  