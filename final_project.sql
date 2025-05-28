with common_table as (
    select gp.user_id as user_id,
           gp.game_name as game_name, 
           gp.revenue_amount_usd as revenue_amount_usd,
           gpu.language as language,
           gpu.age as age,
           gpu.has_older_device_model,
           DATE_TRUNC('month', payment_date)::DATE as payment_date
    from project.games_payments gp
    join project.games_paid_users gpu on gpu.game_name = gp.game_name and gpu.user_id = gp.user_id
),
new_data as (
	select  user_id,
		min(payment_date) as first_payment_date,
		max(payment_date) as last_payment_date,
	EXTRACT(YEAR FROM AGE(max(payment_date), min(payment_date))) * 12 + EXTRACT(MONTH FROM AGE(max(payment_date), min(payment_date))) + 1 as duration
	from common_table
	group by  user_id
),
test_data as ( 
	select ct.user_id, ct.payment_date,first_payment_date,last_payment_date
	from common_table ct
join new_data nd on nd.user_id = ct.user_id
)
select payment_date,
	COUNT(DISTINCT CASE WHEN payment_date = first_payment_date THEN user_id END) AS new_paid_users
from test_data
group by payment_date


monthly_revenue as (					
    select payment_date,
    	round(sum(revenue_amount_usd),0) as mrr,
    	count(distinct user_id) as paid_users,
    	round(sum(revenue_amount_usd) / count(distinct user_id),0) as ARPPU,
    	COUNT(DISTINCT CASE WHEN payment_date = (SELECT first_payment_date FROM new_data where new_data.user_id=common_table.user_id) THEN user_id END) AS new_paid_users,
    	round(sum(CASE WHEN payment_date = (SELECT first_payment_date FROM new_data where new_data.user_id=common_table.user_id ) THEN revenue_amount_usd END),0) AS new_mrr,
    	COUNT(DISTINCT CASE WHEN payment_date = (SELECT last_payment_date FROM new_data where new_data.user_id=common_table.user_id) THEN user_id END) AS Churned_Users,
    	round(sum(CASE WHEN payment_date = (SELECT last_payment_date FROM new_data where new_data.user_id=common_table.user_id) THEN revenue_amount_usd END),0) AS Churned_revenue
    from common_table
    group by payment_date
),
  churn_rate as (
	select payment_date,
		100 * round(cast(Churned_Users as decimal)/ cast(lag(paid_users, 1, NULL) over (order by payment_date) as decimal), 2) as churn_rate
	from monthly_revenue
),
  subscrp_dates as (
	select user_id,game_name,payment_date,
		sum(revenue_amount_usd) as revenue_amount_usd
	from common_table ct
	group by user_id, game_name, payment_date
), 
  subscrp_date as (
	select 
	payment_date, 
		sum(revenue_amount_usd) as revenue_amount_usd,
		sum(case when (select count(*) from subscrp_dates ct1 where ct1.user_id = ct.user_id and  ct1.game_name = ct.game_name and  ct1.payment_date = ct.payment_date::DATE - INTERVAL '1 month') > 0 then (select revenue_amount_usd from subscrp_dates ct1 where ct1.user_id = ct.user_id and  ct1.game_name = ct.game_name and  ct1.payment_date = ct.payment_date::DATE - INTERVAL '1 month') end) as previous_month_revenue
	from subscrp_dates ct
	group by user_id, game_name, payment_date
),
 mrr_dates as (
	select payment_date,
		sum(case when previous_month_revenue is null then 0 else (case when revenue_amount_usd < previous_month_revenue then (revenue_amount_usd - previous_month_revenue) else 0 end) end) as contraction_mrr,
		sum(case when previous_month_revenue is null then 0 else (case when revenue_amount_usd > previous_month_revenue then (revenue_amount_usd - previous_month_revenue) else 0 end) end) as expansion_mrr
	from subscrp_date
	group by payment_date
)	
select  mr.payment_date, mrr,
		paid_users, ARPPU, 
		new_paid_users, 
		new_mrr,Churned_Users,
		Churned_revenue,churn_rate,
		contraction_mrr,expansion_mrr
from monthly_revenue mr
join churn_rate cr on cr.payment_date = mr.payment_date 
join mrr_dates md on md.payment_date = mr.payment_date
