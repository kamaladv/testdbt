with addtl_protocol_org as (
select 
  poag.protocol_staff_role_id,
  listagg(o.name,'; ') within group (order by o.name) additional_protocol_access_organization
from 
  datalake_raw.oncore_int_raw_data_analytics.organization o
join
  datalake_raw.oncore_int_raw_data_analytics.pcl_oag_accessible_org poao on poao.organization_id = o.organization_id
join
  datalake_raw.oncore_int_raw_data_analytics.pcl_staff_role_org_access_grp psroag on poao.pcl_org_access_group_id = psroag.pcl_org_access_group_id
join
  datalake_raw.oncore_int_raw_data_analytics.pcl_organization_access_group poag on psroag.pcl_org_access_group_id = poag.pcl_org_access_group_id
where  
  poao._fivetran_deleted = false
and
  psroag._fivetran_deleted = false
and
  poag._fivetran_deleted = false    
and    
  poag.protocol_staff_role_id is not null
group by
  poag.protocol_staff_role_id
)
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
  case     
    when random_admin.verify_random_questionnaire = 1 then 'Yes'
    when random_admin.verify_random_questionnaire = 0 then 'No'
    else 'Not applicable'
  end can_verify_randomization_questionnaire,
  case     
    when random_admin.assign_randomized_treatment = 1 then 'Yes'
    when random_admin.assign_randomized_treatment = 0 then 'No'
    else 'Not applicable'
  end can_assign_randomized_treatment,
  case     
    when random_admin.manually_assign_treatment = 1 then 'Yes'
    when random_admin.manually_assign_treatment = 0 then 'No'
    else 'Not applicable'
  end can_manually_assign_treatment,
   case     
    when random_admin.see_blinded_treatment = 1 then 'Yes'
    when random_admin.see_blinded_treatment = 0 then 'No'
    else 'Not applicable'
  end can_see_blinded_treatment,    
  sr.created_user protocol_staff_created_username,  
  sr.created_date protocol_staff_created_date,
  sr.modified_user protocol_staff_modified_username,
  sr.modified_date protocol_staff_modified_date
from 
  datalake_raw.oncore_int_raw_data_analytics.smrs_pcl_staff_role sr
join
  datalake_raw.oncore_int_raw_data_analytics.pf_code r on r.category = 'STAFF_ROLE' and sr.staff_role = r.code_id
left join
  datalake_raw.oncore_int_raw_data_analytics.pcl_staff_rand_access_config random_admin on sr.protocol_staff_role_id = random_admin.protocol_staff_role_id
left join
  addtl_protocol_org apo on sr.protocol_staff_role_id = apo.protocol_staff_role_id
where
   sr._fivetran_deleted = false