

{{
    config(
        materialized='ephemeral'
    )
}}

with pcl_staff_role as (
    select
  protocol_staff_role_id,
  
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
  end can_see_blinded_treatment
  from {{ source('oncore_src','pcl_staff_rand_access_config') }} random_admin
  where _fivetran_deleted = false )

  select * from pcl_staff_role