-- ===================================================
-- create dune_user_generated.dex_pool_info table, collect all info for pools
CREATE or REPLACE view dune_user_generated.dex_pool_info as  
with uniswap_v2 AS (
    SELECT pair as pool, contract_address, token0, token1, 0.3 as fee
        ,'Uniswap v2' AS project
        , evt_block_time as block_time
    FROM uniswap_v2."Factory_evt_PairCreated"
    )
, uniswap_v3 AS (
    SELECT pool, contract_address, token0, token1, fee/1e4 as fee
        ,'Uniswap v3' AS project
        , evt_block_time as block_time
    FROM uniswap_v3."Factory_evt_PoolCreated"
    )

, sushiswap AS (
    SELECT pair as pool, contract_address, token0, token1, 0.3 as fee
        ,'SushiSwap' AS project
        , evt_block_time as block_time
    FROM sushi."Factory_evt_PairCreated"
    )

, all_pools AS (
    SELECT * FROM uniswap_v2
    UNION ALL
    SELECT * FROM uniswap_v3
    UNION ALL
    SELECT * FROM sushiswap
    ) 
, liquidity as (select pool_address, token_pool_percentage
    , MAX(case when token_index = 'token_0' then token_symbol end) as token0_symbol
    , MAX(case when token_index = 'token_0' then token_amount end) as token0_amount
    , MAX(case when token_index = 'token_0' then token_usd_amount end) as token0_usd_amount
    , MAX(case when token_index = 'token_1' then token_symbol end) as token1_symbol
    , MAX(case when token_index = 'token_1' then token_amount end) as token1_amount
    , MAX(case when token_index = 'token_1' then token_usd_amount end) as token1_usd_amount
    , sum(token_usd_amount) as liquidity
    from dex."liquidity" l
    where day = current_date - INTERVAL '1 day'
    group by 1,2)
, current_supply AS (
SELECT symbol, 1 as is_stable, token_address, SUM(amount) AS total_supply
FROM (
    SELECT  symbol, token_address, -SUM(amount) AS amount
    FROM stablecoin."burn"
    GROUP BY 1,2
    
    UNION ALL
    
    SELECT symbol, token_address, SUM(amount) AS amount
    FROM stablecoin."mint"
    GROUP BY 1,2
) current
GROUP BY 1,2,3 
)
, last_step as (
select a.*, l.token_pool_percentage
    ,coalesce(l.token0_symbol,s.symbol) as token0_symbol
    ,l.token0_amount
    ,l.token0_usd_amount
    ,coalesce(l.token1_symbol, c.symbol) as token1_symbol
    ,l.token1_amount
    ,l.token1_usd_amount
    ,l.liquidity
,s.total_supply as token0_total_supply
,COALESCE(s.is_stable,0) as is_token0_stable
,c.total_supply as token1_total_supply
,COALESCE(c.is_stable,0) as is_token1_stable
from all_pools a 
left join liquidity l on a.pool = l.pool_address
left join current_supply s on a.token0 = s.token_address
left join current_supply c on a.token0 = c.token_address
)
select *
-- , datediff(block_time, current_date) as created_days
, case when is_token0_stable = 1 and is_token1_stable = 1 then 'stable to stable'
    when is_token0_stable = 1 and is_token1_stable = 0 then 'stable to non-stable'
    when is_token0_stable = 0 and is_token1_stable = 1 then 'non-stable to stable'
    when is_token0_stable = 0 and is_token1_stable = 0 then 'non-stable to non-stable'
    end as pool_type
from last_step
;
