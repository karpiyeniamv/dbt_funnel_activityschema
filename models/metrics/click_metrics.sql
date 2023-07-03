with final as
     (
               select
                         customer
                       , cast(JSON_VALUE(channel) as string) as channel
                       , cast(JSON_VALUE(campaign_id) as string) as campaign_id
                       , cast(JSON_VALUE(product_id) as string) as product_id
                       , cast(JSON_VALUE(employee) as string) as employee
                       , cast(JSON_VALUE(contact_type) as string) as contact_type
                       , cast(JSON_VALUE(contact_duration) as integer) as contact_duration
                       , cast(JSON_VALUE(revenue) as numeric) as revenue
               from
                         (
                                select
                                       ts
                                     , customer
                                     , feature_json['channel']     as channel
                                     , feature_json['campaign_id'] as campaign_id
                                     , activity_occurrence
                                     , activity_repeated_at
                                from
                                       {{ref('dim_events')}}
                                where
                                       activity = 'click'
                         )
                         as A
                         left join
                                   (
                                          select
                                                 ts
                                               , feature_json['product_id']   as product_id
                                               , feature_json['contact_type'] as contact_type
                                               , activity_occurrence
                                               , activity_repeated_at
                                          from
                                                 {{ref('dim_events')}}
                                          where
                                                 activity = 'lead'
                                   )
                                   as B
                                   on
                                             (
                                                       B.ts=A.activity_repeated_at
                                             )
                         left join
                                   (
                                          select
                                                 ts
                                               , feature_json['employee']         as employee
                                               , feature_json['contact_duration'] as contact_duration
                                               , activity_occurrence
                                               , activity_repeated_at
                                          from
                                                 {{ref('dim_events')}}
                                          where
                                                 activity = 'consultation'
                                   )
                                   as C
                                   on
                                             (
                                                       C.ts=B.activity_repeated_at
                                             )
                         left join
                                   (
                                          select
                                                 ts
                                               , feature_json['revenue'] as revenue
                                               , activity_occurrence
                                               , activity_repeated_at
                                          from
                                                 {{ref('dim_events')}}
                                          where
                                                 activity = 'completed_order'
                                   )
                                   as D
                                   on
                                             (
                                                       D.ts=C.activity_repeated_at
                                             )
     )
select *
from
       final