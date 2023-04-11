80,107,783,903 USDT
32,610,838,881 USDC
7,157,628,171 BUSD
5,281,904,941 DAI
2,085,284,867 TUSD
878,084,065 USDP
725,332,036 USDD
607,049,883 GUSD
424,996,178 FEI
9,799,455,409 USTC
453,448,622 TRIBE
1,044,853,133 FRAX
273,145,279 USDJ
269,316,274 LUSD
124,125,940 EURS
4,771,015,133 vUSDC
111,567,264 USDX
96,391,260 XSGD
2,694,770,470 vBUSD
57,498,554 VAI
45,790,671 SUSD
40,783,541 CUSD
34,898,106 OUSD
12,071,671 SBD
31,150,654 EUROC
1,486,153,169 vUSDT
28,600,072 USDK
28,850,667 RSV
38,345,548,644 KRT
2,733,961,999 GYEN
17,865,022 CEUR
225,715,436,474 BIDR
222,593,861 HUSD
121,048,927,413 IDRT
274,537,625 vDAI
73,874 DGD
28,245,600 BITCNY
3,100,000 XCHF
54,623 DGX
2,642,505 EOSDT
1,472,237 ZUSD
446,012,145 ESD
461,968 USDS
54,575,145 BAC
--
44,262,913 MIMATIC
40,001,429 EURT
0 USDP
27,055,701 AGEUR
--

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


  
, tokens AS (SELECT CASE WHEN token_a_symbol > token_b_symbol THEN token_b_symbol ELSE token_a_symbol END AS "Token A"
    , CASE WHEN token_a_symbol > token_b_symbol THEN token_a_symbol ELSE token_b_symbol END AS "Token B"
    , exchange_contract_address AS contract_address
    FROM (SELECT * FROM dex.trades
    WHERE exchange_contract_address=(SELECT pc FROM pool_contract)
    LIMIT 1) t
    GROUP BY exchange_contract_address, token_a_symbol, token_b_symbol
    )


SELECT MAX("Token A") AS "Token A"
, MAX("Token B") AS "Token B"
, MAX(dpf.project) AS "Project"
, MAX(dpf.lp_fee_percentage) AS "Pool Fee"
FROM dex_pool_fees dpf
LEFT JOIN tokens dt ON dt.contract_address=dpf.contract_address
AND end_block_time > NOW()
LIMIT 1







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


