{{
    config(
        materialized='ephemeral'
    )
}}

with addtl_protocol_org as (
select 
  poag.protocol_staff_role_id,
  listagg(o.name,';') within group (order by o.name) additional_protocol_access_organization
from 
  {{ source('oncore_src','organization') }} o
join
  {{ source('oncore_src','pcl_oag_accessible_org') }} poao on poao.organization_id = o.organization_id
join
  {{ source('oncore_src','pcl_staff_role_org_access_grp') }} psroag on poao.pcl_org_access_group_id = psroag.pcl_org_access_group_id
join
  {{ source('oncore_src','pcl_organization_access_group') }} poag on psroag.pcl_org_access_group_id = poag.pcl_org_access_group_id
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

select * from addtl_protocol_org