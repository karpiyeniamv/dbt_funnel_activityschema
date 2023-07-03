with final as
(
    select 
    A.employee,
    cast(A.cnt/B.cnt as numeric) as employee_conversion
    from 
    (
    select employee, count(*) as cnt from  {{ref('order_metrics')}} group by employee) as A
    inner join
    (
    select employee, count(*) as cnt from  {{ref('click_metrics')}} group by employee) as B
    on
    A.employee=B.employee

)
select *
from
       final