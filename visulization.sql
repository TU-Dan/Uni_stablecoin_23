-- all pools: pool type distribution
select pool_type, count(distinct pool) as pool_cnt, sum(liquidity) as pool_liquidity
from dune_user_generated.dex_pool_info
-- where liquidity > 100
group by pool_type;

-- pools with liquidity > 100: pool type distribution
select pool_type, count(distinct pool) as pool_cnt, sum(liquidity) as pool_liquidity
from dune_user_generated.dex_pool_info
-- where liquidity > 100
group by pool_type;

-- pools with liquidity > 100000: pool type distribution
select pool_type, count(distinct pool) as pool_cnt, sum(liquidity) as pool_liquidity
from dune_user_generated.dex_pool_info
-- where liquidity > 100000
group by pool_type;

-- top pools:
-- top 10 pools by pool type
with tmp as (select pool, token0_symbol, token1_symbol,liquidity, pool_type
, rank() OVER (PARTITION BY pool_type ORDER BY liquidity DESC) AS liq_rank_type
from dune_user_generated.dex_pool_info1
where liquidity > 1000
)
select *
from tmp
where liq_rank_type <= 10
;

-- top pools by pool type
with tmp as (
select pool, token0_symbol, token1_symbol,liquidity, pool_type
, rank() OVER (PARTITION BY pool_type ORDER BY liquidity DESC) AS liq_rank_type
from dune_user_generated.dex_pool_info1
where liquidity > 0
)
,all_pool as (
select pool_type
, sum(liquidity) as liquidity
from tmp
group by pool_type
)
,top1 as (
select pool_type
, sum(liquidity) as liquidity_top1
from tmp
where liq_rank_type = 1
group by pool_type
)
,top3 as (
select pool_type
, sum(liquidity) as liquidity_top3
from tmp
where liq_rank_type <= 3
group by pool_type
)
,top10 as (
select pool_type
, sum(liquidity) as liquidity_top10
from tmp
where liq_rank_type <= 10
group by pool_type
)
,top20 as (
select pool_type
, sum(liquidity) as liquidity_top20
from tmp
where liq_rank_type <= 20
group by pool_type
)
select a.pool_type
, b.liquidity_top1
, c.liquidity_top3
, d.liquidity_top10
, e.liquidity_top20
, a.liquidity
, b.liquidity_top1/a.liquidity as liquidity_top1_percentage
, c.liquidity_top3/a.liquidity as liquidity_top3_percentage
, d.liquidity_top10/a.liquidity as liquidity_top10_percentage
, e.liquidity_top20/a.liquidity as liquidity_top20_percentage
from all_pool a
left join top1 b 
on a.pool_type = b.pool_type
left join top3 c
on a.pool_type = c.pool_type
left join top10 d 
on a.pool_type = d.pool_type
left join top20 e
on a.pool_type = e.pool_type
;

-- top pools stacked by token0 & token1
with tmp as (
select token0_symbol, token1_symbol, pool_type, sum(liquidity) as liquidity
, rank() OVER (PARTITION BY pool_type ORDER BY sum(liquidity) DESC) AS liq_rank_token
from dune_user_generated.dex_pool_info1
where liquidity > 0
group by 1,2,3
)
,all_pool as (
select pool_type
, sum(liquidity) as liquidity
from tmp
group by pool_type
)
,top1 as (
select pool_type
, sum(liquidity) as liquidity_top1
from tmp
where liq_rank_token = 1
group by pool_type
)
,top3 as (
select pool_type
, sum(liquidity) as liquidity_top3
from tmp
where liq_rank_token <= 3
group by pool_type
)
,top10 as (
select pool_type
, sum(liquidity) as liquidity_top10
from tmp
where liq_rank_token <= 10
group by pool_type
)
,top20 as (
select pool_type
, sum(liquidity) as liquidity_top20
from tmp
where liq_rank_token <= 20
group by pool_type
)
select a.pool_type
, b.liquidity_top1
, c.liquidity_top3
, d.liquidity_top10
, e.liquidity_top20
, a.liquidity
, b.liquidity_top1/a.liquidity as liquidity_top1_percentage
, c.liquidity_top3/a.liquidity as liquidity_top3_percentage
, d.liquidity_top10/a.liquidity as liquidity_top10_percentage
, e.liquidity_top20/a.liquidity as liquidity_top20_percentage
from all_pool a
left join top1 b 
on a.pool_type = b.pool_type
left join top3 c
on a.pool_type = c.pool_type
left join top10 d 
on a.pool_type = d.pool_type
left join top20 e
on a.pool_type = e.pool_type
;


-- top 10 pairs by pool type
with tmp as (
select token0_symbol, token1_symbol, pool_type, sum(liquidity) as liquidity
, rank() OVER (PARTITION BY pool_type ORDER BY sum(liquidity) DESC) AS liq_rank_token
from dune_user_generated.dex_pool_info1
where liquidity > 0
group by 1,2,3
)
select liq_rank_token as "Pair Rank", token0_symbol as "TokenA"
, token1_symbol as "TokenB",liquidity as "Liquidity"
from tmp
where liq_rank_token <= 10
and pool_type = 'stable to stable'
;


with tmp as (
select token1_symbol, pool_type, sum(liquidity) as liquidity
, rank() OVER (PARTITION BY pool_type ORDER BY sum(liquidity) DESC) AS liq_rank_token
from dune_user_generated.dex_pool_info1
where liquidity > 0
group by 1,2
)
select liq_rank_token, token1_symbol, liquidity
from tmp
where liq_rank_token <= 3
and pool_type = 'non-stable to non-stable'


-- pool trade volume by time
with non_non as (SELECT date_trunc('day', block_time) as day
, sum(usd_amount) "non-stable to non-stable"
FROM dex.trades dt
WHERE exchange_contract_address in (
            SELECT pool
            FROM dune_user_generated.dex_pool_info1 
            where pool_type = 'non-stable to non-stable')
AND dt.category='DEX'
AND block_time >= '2020-04-10'
group by 1
) 
, non_sta as (SELECT date_trunc('day', block_time) as day
, sum(usd_amount) "non-stable to stable"
FROM dex.trades dt
WHERE exchange_contract_address in (
            SELECT pool
            FROM dune_user_generated.dex_pool_info1 
            where pool_type = 'non-stable to stable')
AND dt.category='DEX'
AND block_time >= '2020-04-10'
group by 1
)
,sta_non as (SELECT date_trunc('day', block_time) as day
, sum(usd_amount) "stable to non-stable"
FROM dex.trades dt
WHERE exchange_contract_address in (
            SELECT pool
            FROM dune_user_generated.dex_pool_info1 
            where pool_type = 'stable to non-stable')
AND dt.category='DEX'
AND block_time >= '2020-04-10'
group by 1
)
, sta_sta as (SELECT date_trunc('day', block_time) as day
, sum(usd_amount) "stable to stable"
FROM dex.trades dt
WHERE exchange_contract_address in (
            SELECT pool
            FROM dune_user_generated.dex_pool_info1 
            where pool_type = 'stable to stable')
AND dt.category='DEX'
AND block_time >= '2020-04-10'
group by 1
)
, all_type as (
select a.day, a."non-stable to non-stable", b."non-stable to stable", c."stable to non-stable", d."stable to stable"
from non_non a 
left join non_sta b on a.day = b.day
left join sta_non c on a.day = c.day
left join sta_sta d on a.day = d.day
order by a.day
)
select *
, ("non-stable to non-stable"/ LAG("non-stable to non-stable") OVER (ORDER BY day)) - 1 AS non_non_rate
, ("non-stable to stable"/ LAG("non-stable to stable") OVER (ORDER BY day)) - 1 AS non_sta_rate
, ("stable to non-stable"/ LAG("stable to non-stable") OVER (ORDER BY day)) - 1 AS sta_non_rate
, ("stable to stable"/ LAG("stable to stable") OVER (ORDER BY day)) - 1 AS sta_sta_rate
from all_type
;



-- pool liquidity by time
, liquidity as (
    select pool_address, token_pool_percentage,token_index, token_symbol, token_amount, token_usd_amount
    from dex."liquidity" l
    where day = (SELECT MAX(day) FROM dex."liquidity" )
    -- current_date - INTERVAL '1 day'
    )

with non_non as (SELECT  day
, sum(token_usd_amount) "non-stable to non-stable"
FROM dex."liquidity" 
WHERE pool_address in (
            SELECT pool
            FROM dune_user_generated.dex_pool_info1 
            where pool_type = 'non-stable to non-stable')
AND day >= '2021-01-01'
group by 1
) 
, non_sta as (SELECT day
, sum(token_usd_amount) "non-stable to stable"
FROM dex."liquidity"
WHERE pool_address in (
            SELECT pool
            FROM dune_user_generated.dex_pool_info1 
            where pool_type = 'non-stable to stable')
AND day >= '2021-01-01'
group by 1
)
,sta_non as (SELECT day
, sum(token_usd_amount) "stable to non-stable"
FROM dex."liquidity"
WHERE pool_address in (
            SELECT pool
            FROM dune_user_generated.dex_pool_info1 
            where pool_type = 'stable to non-stable')
AND day >= '2021-01-01'
group by 1
)
, sta_sta as (SELECT day
, sum(token_usd_amount) "stable to stable"
FROM dex."liquidity"
WHERE pool_address in (
            SELECT pool
            FROM dune_user_generated.dex_pool_info1 
            where pool_type = 'stable to stable')
AND day >= '2021-01-01'
group by 1
)
, all_type as (
select a.day, a."non-stable to non-stable", b."non-stable to stable", c."stable to non-stable", d."stable to stable"
from non_non a 
left join non_sta b on a.day = b.day
left join sta_non c on a.day = c.day
left join sta_sta d on a.day = d.day
order by a.day
)
select *
, ("non-stable to non-stable"/ LAG("non-stable to non-stable") OVER (ORDER BY day)) - 1 AS non_non_rate
, ("non-stable to stable"/ LAG("non-stable to stable") OVER (ORDER BY day)) - 1 AS non_sta_rate
, ("stable to non-stable"/ LAG("stable to non-stable") OVER (ORDER BY day)) - 1 AS sta_non_rate
, ("stable to stable"/ LAG("stable to stable") OVER (ORDER BY day)) - 1 AS sta_sta_rate
from all_type
;


-- has data flaws in the database



-- trade volume change rate during depegs
-- pool type count & liquidity change rate during depegs