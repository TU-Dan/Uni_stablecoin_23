WITH pool_contract AS (SELECT CASE WHEN LEFT('{{Pool}}', 1) = '\' THEN '{{Pool}}'::bytea 
    WHEN LEFT('{{Pool}}', 1) = '0' THEN ('\' || RIGHT('{{Pool}}', -1))::bytea
    END AS pc
    )

, start_and_end_date AS (
    SELECT CASE WHEN dune_user_generated.is_date('{{Since}}')='true' AND '{{Since}}' < NOW() THEN '{{Since}}'::timestamp
        ELSE '2000-01-01'::timestamp END AS sd,
    CASE WHEN dune_user_generated.is_date('{{Until}}')='true' AND '{{Until}}' < NOW() THEN '{{Until}}'::timestamp
        ELSE NOW() END AS ed
    )

, uniswap_v1 AS (
    SELECT exchange AS contract_address
    , 'Uniswap v1' AS project
    , 0.3 AS lp_fee_percentage
    , evt_block_time AS block_time
    FROM uniswap."Factory_evt_NewExchange")
    
, uniswap_v2 AS (
    SELECT pair AS contract_address
    , 'Uniswap v2' AS project
    , 0.3 AS lp_fee_percentage
    , evt_block_time AS block_time
    FROM uniswap_v2."Factory_evt_PairCreated"
    )
    
, uniswap_v3 AS (
    SELECT pool AS contract_address
    , 'Uniswap v3' AS project
    , fee/1e4 AS lp_fee_percentage
    , evt_block_time AS block_time
    FROM uniswap_v3."Factory_evt_PoolCreated"
    )

, sushiswap AS (
    SELECT pair AS contract_address
    , 'SushiSwap' AS project
    , 0.3 AS lp_fee_percentage
    , evt_block_time AS block_time
    FROM sushi."Factory_evt_PairCreated"
    )

, curve AS (
    SELECT '\xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7'::bytea AS contract_address
        , 'Curve' AS project
        , 0.04 AS lp_fee_percentage
        , '2020-09-06 20:23'::timestamp AS block_time
        )

, dodo_v1 AS (
    SELECT "dppAddress" AS contract_address
    , 'DODO v1' AS project
    , "lpFeeRate"/1e16 AS lp_fee_percentage
    , call_block_time AS block_time 
    FROM dodo."DPPFactory_call_initDODOPrivatePool" WHERE call_success IS TRUE    
    UNION
    SELECT contract_address
    , 'DODO v1' AS project
    , "newLpFeeRate"/1e16 AS lp_fee_percentage
    , evt_block_time AS block_time 
    FROM  dodo."DPP_evt_LpFeeRateChange"
    )
    
, dodo_dpp AS (
    SELECT "output_newBornDODO" AS contract_address
    , 'DODO v2' AS project
    , "lpFeeRate"/1e16 AS lp_fee_percentage
    , call_block_time AS block_time 
    FROM dodo."DODOZoo_call_breedDODO" WHERE call_success IS TRUE    
    UNION
    SELECT contract_address
    , 'DODO v2' AS project
    , "newLiquidityProviderFeeRate"/1e16 AS lp_fee_percentage
    , evt_block_time AS block_time 
    FROM  dodo."DODO_evt_UpdateLiquidityProviderFeeRate"
    )
    
, dodo_dvm AS (
    SELECT "output_newVendingMachine" AS contract_address
    , 'DODO v2' AS project
    , "lpFeeRate"/1e16 AS lp_fee_percentage
    , call_block_time AS block_time 
    FROM dodo."DVMFactory_call_createDODOVendingMachine" WHERE call_success IS TRUE
    )
    
, dodo_dsp AS (
    SELECT "output_newStablePool" AS contract_address,
        'DODO v2' AS project,
        "lpFeeRate"/1e16 AS lp_fee_percentage,
        call_block_time AS block_time 
    FROM dodo."DSPFactory_call_createDODOStablePool" WHERE call_success IS TRUE
    )

, balancer_v1 AS (
    SELECT contract_address
    , 'Balancer v1' AS project
    , "swapFee"/1e16 AS lp_fee_percentage
    , call_block_time AS block_time
    FROM balancer."BPool_call_setSwapFee" WHERE call_success = 'true'
    )
    
, balancer_v2 AS (
    SELECT contract_address
    , 'Balancer v2' AS project
    , "swapFeePercentage"/1e16 AS lp_fee_percentage
    , evt_block_time AS block_time
    FROM balancer_v2."WeightedPool_evt_SwapFeePercentageChanged"
    )
 
, all_pools AS (SELECT * FROM (
    SELECT * FROM uniswap_v1
    UNION ALL
    SELECT * FROM uniswap_v2
    UNION ALL
    SELECT * FROM uniswap_v3
    UNION ALL
    SELECT * FROM sushiswap
    UNION ALL
    SELECT * FROM curve
    UNION ALL
    SELECT * FROM dodo_v1
    UNION ALL
    SELECT * FROM dodo_dpp
    UNION ALL
    SELECT * FROM dodo_dvm
    UNION ALL
    SELECT * FROM dodo_dsp
    UNION ALL
    SELECT * FROM balancer_v1
    UNION ALL
    SELECT * FROM balancer_v2) joined
    WHERE contract_address =  (SELECT pc FROM pool_contract)
    ) 
    
, dex_pool_fees AS (SELECT DISTINCT ON (start_lp.contract_address, start_lp.block_time) start_lp.contract_address AS contract_address
    , start_lp.project
    , start_lp.lp_fee_percentage
    , start_lp.block_time AS start_block_time
    , COALESCE(end_lp.block_time, '3000-01-01') AS end_block_time
    FROM (
        SELECT * FROM all_pools ORDER BY "contract_address", block_time
        ) start_lp
    LEFT JOIN (
        SELECT * FROM all_pools ORDER BY "contract_address", block_time
        ) end_lp
    ON start_lp.contract_address = end_lp.contract_address AND start_lp.block_time < end_lp.block_time
    )


SELECT SUM(usd_amount) AS "USD Volume"
, SUM(usd_amount*lp_fee_percentage) AS "Accumulated Fees"
FROM dex.trades dt
LEFT JOIN dex_pool_fees dpf ON dpf.contract_address = dt.exchange_contract_address
    AND (dt.block_time BETWEEN dpf.start_block_time AND dpf.end_block_time)
WHERE exchange_contract_address = (SELECT pc FROM pool_contract)
AND dt.category='DEX'
AND block_time >= (SELECT sd FROM start_and_end_date)
AND block_time <= (SELECT ed FROM start_and_end_date)

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



trades as (
    select exchange_contract_address, 
    sum(usd_amount) as volume, 
    count(*) as swaps
    from dex."trades"
    where block_time >= current_date - INTERVAL '1 month'
    group by 1
)
inner join trades t on p.pool = t.exchange_contract_address






SELECT SUM(usd_amount) AS "USD Volume"
, SUM(usd_amount*lp_fee_percentage) AS "Accumulated Fees"
FROM dex.trades dt
LEFT JOIN dex_pool_fees dpf ON dpf.contract_address = dt.exchange_contract_address
    AND (dt.block_time BETWEEN dpf.start_block_time AND dpf.end_block_time)
WHERE exchange_contract_address = (SELECT pc FROM pool_contract)
AND dt.category='DEX'
AND block_time >= (SELECT sd FROM start_and_end_date)
AND block_time <= (SELECT ed FROM start_and_end_date)



select evt_block_time,
    token0_symbol || '-' || token1_symbol || ' ' || (fee / 1e4)::string || '%' as pool_name,
    '<a href=https://etherscan.io/address/' || pool || ' target=_blank>' || pool || '</a>' as pool_link,
    token0,
    token1,
    fee,
    evt_tx_hash
from last_crated_pools
order by evt_block_time desc


lp as (
    select day, pool_address, sum(token_usd_amount) as liquidity
    from dex."liquidity" l
    where day = current_date - INTERVAL '1 day'
    group by 1, 2
)
liquidity as "TVL"



    select pool_address, token_pool_percentage
    , MAX(case when token_index = 'token_0' then token_symbol end) as token0_symbol
    , MAX(case when token_index = 'token_0' then token_amount end) as token0_amount
    , MAX(case when token_index = 'token_0' then token_usd_amount end) as token0_usd_amount
    , MAX(case when token_index = 'token_1' then token_symbol end) as token1_symbol
    , MAX(case when token_index = 'token_1' then token_amount end) as token1_amount
    , MAX(case when token_index = 'token_1' then token_usd_amount end) as token1_usd_amount
    , sum(token_usd_amount) as liquidity
    from dex."liquidity" l
    where day = current_date - INTERVAL '1 day'
    group by 1,2

