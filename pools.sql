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
-- no token A B right now, need to add them
with uniswap_v1 AS (
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

, all_pools AS (SELECT * FROM (
    SELECT * FROM uniswap_v1
    UNION ALL
    SELECT * FROM uniswap_v2
    UNION ALL
    SELECT * FROM uniswap_v3
    UNION ALL
    SELECT * FROM sushiswap) joined
    -- WHERE contract_address =  (SELECT pc FROM pool_contract)
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