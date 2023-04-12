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