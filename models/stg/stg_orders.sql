with final as
     (
            select
                   activity_id
                 , ts
                 , activity
                 , customer
                 , {{ activity_schema.feature_json(['revenue', 'order_id']) }}
                 , null as anonymous_customer_id
                 , null as link
                 , null as revenue_impact
                 , null as tag
                 , null as subject
            from
                   {{ref('orders')}}
     )
select *
from
       final