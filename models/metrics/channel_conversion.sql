with final as
(
    select 
    A.channel,
    cast(A.cnt/B.cnt as numeric) as channel_conversion
    from 
    (
    select channel, count(*) as cnt from  {{ref('order_metrics')}} group by channel) as A
    inner join
    (
    select channel, count(*) as cnt from  {{ref('click_metrics')}} group by channel) as B
    on
    A.channel=B.channel

)
select *
from
       final