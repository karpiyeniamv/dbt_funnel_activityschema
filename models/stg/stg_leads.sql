with final as
     (
            select
                   activity_id
                 , ts
                 , activity
                 , customer
                 , {{ activity_schema.feature_json(['lead_source', 'product_id', 'contact_person', 'contact_type', 'contact_info']) }}
                 , null as anonymous_customer_id
                 , null as link
                 , null as revenue_impact
                 , null as tag
                 , null as subject
            from
                   {{ref('leads')}}
     )
select *
from
       final