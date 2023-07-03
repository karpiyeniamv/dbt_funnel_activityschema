with final as
     (
            select *
            from
                   {{ref('stg_clicks')}}
            union all
            select *
            from
                   {{ref('stg_leads')}}
            union all
            select *
            from
                   {{ref('stg_consultations')}}
            union all
            select *
            from
                   {{ref('stg_orders')}}
     )
select
       ts
     , activity
     , customer
     , feature_json
     , {{ activity_schema.activity_occurrence() }}
from
    final