
-- 1. write a query to print top 5 cities with highest spends and their percentage contribution of total credit card spends 


with tempcte as
(
select *,
sum(cast(amount as bigint)) over() as overallspend
from creditcard
)
select top 5 city, sum(amount) as totalspent,  
CAST(ROUND(1.0 * sum(amount)/overallspend * 100,2) as decimal(10,3)) as percent_contribution
from tempcte
group by city, overallspend
order by sum(amount) desc



-------


-- 2. write a query to print highest spend month and amount spent in that month for each card type


with tempcte as
(
select *, datename(month, date) as months from creditcard
where datename(month, date) in (select top 1 datename(month, date) as months from creditcard
group by datename(month, date)
order by sum(amount) desc)
)
select months, Card_Type, sum(Amount) as totalspent from tempcte
group by months, Card_Type




-------


--3. write a query to print the transaction details(all columns from the table) for each card type when it reaches a cumulative of 
   -- 1000000 total spends(We should have 4 rows in the o/p one for each card type)


with tempcte as
(
select *,
sum(amount) over(partition by city, card_type order by amount) as running_sum
from creditcard
), tempabc as (
select * from tempcte
where running_sum >= 1000000
), ranki as
(
select *, ROW_NUMBER() over(partition by city, card_type order by running_sum) as ranks from tempabc
)
select * from ranki
where ranks = 1




-------


-- 4. write a query to find city which had lowest percentage spend for gold card type


with tempcte as
(
select city, sum(amount) as totalspent, sum(sum(amount)) over() as overall_spent from creditcard
where card_type = 'Gold'
group by city
)
select top 1 *, 1.0 * totalspent/overall_spent * 100 as percent_spent from tempcte
order by totalspent asc




-------


-- 5. write a query to print 3 columns: city, highest_expense_type , lowest_expense_type (example format : Delhi , bills, Fuel)


with tempcte as
(
select city, Exp_Type, sum(amount) as totalspent from creditcard
group by city, Exp_Type
), tempabc as (
select *,
ROW_NUMBER() over(partition by city order by totalspent asc) as l_expense,
ROW_NUMBER() over(partition by city order by totalspent desc) as h_expense
from tempcte
), tempfinal as (
select * from tempabc
where l_expense = 1 or h_expense=1
), tempexpense as (
select city, Exp_Type, totalspent, case when l_expense = 1 then 1 else 0 end as low_expense,
case when h_expense = 1 then 1 else 0 end as high_expense
from tempfinal
), tempfinals as
(
select *,
case when low_expense = 1 then Exp_Type end as low_type,
case when high_expense = 1 then Exp_Type end as high_type
from tempexpense
), tempresult as
(
select a.city, a.Exp_Type as aExp_Type, a.low_type as alow_type, a.high_type as ahigh_type, 
b.Exp_Type as bExp_Type, b.low_type as blow_type, b.high_type as bhigh_type  from tempfinals as a
join tempfinals as b
on a.city = b.city
WHERE a.Exp_Type != b.Exp_Type
)
select city, ahigh_type as highest_expense_type,blow_type lowest_expense_type from tempresult
where ahigh_type is not null and blow_type is not null



-------


-- 6. write a query to find percentage contribution of spends by females for each expense type


with tempcte as
(
select Exp_Type, sum(Amount) as total_spent_female, (select sum(cast(amount as bigint)) from creditcard where Gender = 'F') as total_female from creditcard
where Gender = 'F'
group by Exp_Type
)
select *, 1.0 * total_spent_female/total_female * 100 as percent_spent from tempcte



-------



-- 7. which card and expense type combination saw highest month over month growth in Jan-2014


with tempcte as(
select year(date) as years, datename(month,date) as months,Card_Type, Exp_Type, sum(amount) as totalspent from creditcard
group by year(date), datename(month,date), Card_Type, Exp_Type
), tempfinal as (
select * from tempcte
where (years = 2013 AND months = 'December') OR (years = 2014 AND months = 'January')
), tempresult as (
select a.years as ayears, a.months as amonths, a.Card_Type as aCard_Type, a.Exp_Type as aExp_Type, a.totalspent as atotalspent, 
b.years as byears, b.months as bmonths, b.Card_Type as bCard_Type, b.Exp_Type as bExp_Type, b.totalspent as btotalspent ,
b.totalspent-a.totalspent as minus
from tempfinal as a
join tempfinal as b
on a.Card_Type = b.Card_Type and a.Exp_Type = b.Exp_Type
)
select *, 1.0 * minus/atotalspent * 100 as percenti from tempresult
order by 1.0 * minus/atotalspent * 100 desc




-------



-- 8. during weekends which city has highest total spend to total no of transcations ratio 


with tempcte as(
select *, DATENAME(dw,date) as theDayName from creditcard
where DATENAME(dw,date) in ('Saturday','Sunday')
), tempabc as
(
select city, count(*) as total_transaction_city_weekend, sum(amount) as spent_weekend from tempcte
group by city
)
select top 1 city, 1.0 * spent_weekend/total_transaction_city_weekend as ratio from tempabc
group by city, spent_weekend, total_transaction_city_weekend
order by spent_weekend/total_transaction_city_weekend desc



-------



-- 9. which city took least number of days to reach its 500th transaction after first transaction in that city


with tempcte as
(
select city, date, count(*) as datecounts from creditcard
group by city, date
), tempfinal as(
select *, sum(datecounts) over(partition by city order by date asc) as runningcount,
ROW_NUMBER() over(partition by city order by city) as ids from tempcte
), tempok as(
select *, ROW_NUMBER() over(partition by city order by ids) as ranki from tempfinal
where runningcount>=500
)
select top 1 * from tempok
where ranki = 1
order by ids




-------



-- 10. write a query to print 3 columns: city, highest_expense_amount , lowest_expense_amount (example format : Delhi , 100, 20)


with tempcte as
(
select city, Exp_Type, sum(amount) as totalspent from creditcard
group by city, Exp_Type
), tempabc as (
select *,
ROW_NUMBER() over(partition by city order by totalspent asc) as l_expense,
ROW_NUMBER() over(partition by city order by totalspent desc) as h_expense
from tempcte
), tempfinal as (
select * from tempabc
where l_expense = 1 or h_expense=1
), tempexpense as (
select city, Exp_Type, totalspent, case when l_expense = 1 then 1 else 0 end as low_expense,
case when h_expense = 1 then 1 else 0 end as high_expense
from tempfinal
), tempmain as (
select city, Exp_Type,
case when low_expense = 1 then totalspent else 0 end as low_spent,
case when high_expense = 1 then totalspent else 0 end as high_spent
from tempexpense
)
select city, sum(low_spent) as highest_expense, sum(high_spent) as lowest_expense from tempmain
group by city



