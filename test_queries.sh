#!/bin/bash

# Test script for subgraph-to-hyperindex query converter
# Run the converter first with: cargo run
# Then run this script: ./test_queries.sh

BASE_URL="http://localhost:3000"
FAILED=0
PASSED=0

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

test_query() {
    local name="$1"
    local query="$2"
    local variables="$3"
    
    echo -n "Testing $name... "
    
    if [ -z "$variables" ]; then
        payload="{\"query\": $query}"
    else
        payload="{\"query\": $query, \"variables\": $variables}"
    fi
    
    response=$(curl -s -X POST "$BASE_URL/" \
        -H "Content-Type: application/json" \
        -d "$payload")
    
    # Check if response contains errors
    if echo "$response" | grep -q '"errors"'; then
        echo -e "${RED}FAILED${NC}"
        echo -e "${YELLOW}Query:${NC} $query"
        echo -e "${YELLOW}Variables:${NC} $variables"
        echo -e "${YELLOW}Error:${NC}"
        echo "$response" | jq '.errors' 2>/dev/null || echo "$response"
        echo ""
        ((FAILED++))
        return 1
    else
        echo -e "${GREEN}PASSED${NC}"
        ((PASSED++))
        return 0
    fi
}

echo "========================================"
echo "Subgraph Query Converter Test Suite"
echo "========================================"
echo ""

# 1. GET_LP_ACTIONS
test_query "1. GET_LP_ACTIONS" \
    '"query getPoolActions($id: Bytes) { lpActions(first: 1000, where: { user: $id }, orderBy: timestamp, orderDirection: desc) { user timestamp } }"' \
    '{"id": "0x98279066957a9eaf627d33983409a548cf3e2207"}'

# 2. GET_LP_NFTs
test_query "2. GET_LP_NFTs" \
    '"query getUserData($trader: Bytes) { lpNFTs(where: { user: $trader }, orderBy: atTime, orderDirection: desc) { id shares } }"' \
    '{"trader": "0x98279066957a9eaf627d33983409a548cf3e2207"}'

# 3. GET_USER_LP_DATA
test_query "3. GET_USER_LP_DATA" \
    '"query getUserData($trader: Bytes) { lpShares(where: { user: $trader }) { id shares } lpNFTs(where: { user: $trader }, orderBy: atTime, orderDirection: desc) { id shares } }"' \
    '{"trader": "0x98279066957a9eaf627d33983409a548cf3e2207"}'

# 4. GET_VAULT_SHARES_DATA
test_query "4. GET_VAULT_SHARES_DATA" \
    '"query getShares($id: Bytes!) { lpShares(first: 1000, where: { user: $id }) { id shares } lpNFTs(first: 1000, where: { user: $id }, orderBy: atTime, orderDirection: desc) { id shares } user(id: $id) { keeperAllowance } }"' \
    '{"id": "0x98279066957a9eaf627d33983409a548cf3e2207"}'

# 5. SHARE_TO_ASSETS_PRICE_DAILIES
test_query "5. SHARE_TO_ASSETS_PRICE_DAILIES" \
    '"query shareToAssetsPriceDailies { shareToAssetsPriceDailies(first: 90, orderBy: timestamp, orderDirection: desc) { id day timestamp sharePrice } }"' \
    ''

# 6. GET_USER_LP_SHARES
test_query "6. GET_USER_LP_SHARES" \
    '"query GetUserLpShares($addresses: [Bytes!]) { lpShares(where: { id_in: $addresses }) { id shares } }"' \
    '{"addresses": ["0x98279066957a9eaf627d33983409a548cf3e2207"]}'

# 7. GET_LEADERBOARD_ORDERS
test_query "7. GET_LEADERBOARD_ORDERS" \
    '"query LeaderboardOrders($skip: Int, $first: Int) { orders(where: { isPending: false, orderAction_not: \"Open\", isCancelled: false }, first: $first, skip: $skip, orderBy: totalProfitPercent, orderDirection: desc) { id trader pair { id from to } } }"' \
    '{"skip": 0, "first": 10}'

# 8. GET_USER_OPENING_ORDERS
test_query "8. GET_USER_OPENING_ORDERS" \
    '"query getUserOpeningOrders($tradeIDs: [String!]!) { orders(where: { tradeID_in: $tradeIDs, orderAction: \"Open\", isPending: false }) { id tradeID devFee vaultFee oracleFee } }"' \
    '{"tradeIDs": ["1", "2", "3"]}'

# 9. GET_HISTORY_CANCELLED
test_query "9. GET_HISTORY_CANCELLED" \
    '"query ListCancelledHistory($trader: Bytes, $since: BigInt) { orders(where: { trader: $trader, isPending: false, executedAt_gte: $since, isCancelled: true }, first: 1000, orderBy: executedAt, orderDirection: desc) { id trader pair { id from to } } }"' \
    '{"trader": "0x98279066957a9eaf627d33983409a548cf3e2207", "since": "0"}'

# 10. GET_HISTORY_WITH_OPERATION
test_query "10. GET_HISTORY_WITH_OPERATION" \
    '"query ListOperationsHistory($trader: Bytes, $since: BigInt, $operation: String) { orders(where: { trader: $trader, isPending: false, executedAt_gte: $since, orderAction: $operation }, first: 1000, orderBy: executedAt, orderDirection: desc) { id trader pair { id from to } } }"' \
    '{"trader": "0x98279066957a9eaf627d33983409a548cf3e2207", "since": "0", "operation": "Open"}'

# 11. GET_HISTORY
test_query "11. GET_HISTORY" \
    '"query ListOrdersHistory($trader: Bytes, $since: BigInt) { orders(where: { trader: $trader, isPending: false, executedAt_gte: $since }, first: 1000, orderBy: executedAt, orderDirection: desc) { id trader pair { id from to } } }"' \
    '{"trader": "0x98279066957a9eaf627d33983409a548cf3e2207", "since": "0"}'

# 12. EXPORT_HISTORY
test_query "12. EXPORT_HISTORY" \
    '"query ListHistory($trader: Bytes) { orders(where: { trader: $trader, isPending: false }, first: 1000, orderBy: executedAt, orderDirection: desc) { id trader pair { id from to } } }"' \
    '{"trader": "0x98279066957a9eaf627d33983409a548cf3e2207"}'

# 13. GET_PUBLIC_TRADES
test_query "13. GET_PUBLIC_TRADES" \
    '"query GetPublicTrades($first: Int, $skip: Int) { orders(first: $first, skip: $skip, orderBy: initiatedAt, orderDirection: desc, where: { isPending: false, isCancelled: false, trader_not: \"0xa02fa4da932f616975f258ce0a9077f8ed350164\" }) { id trader pair { id from to } } }"' \
    '{"first": 10, "skip": 0}'

# 14. GET_OPEN_ORDERS
test_query "14. GET_OPEN_ORDERS" \
    '"query ListOrders($trader: Bytes) { limits(first: 1000, where: { trader: $trader, isActive: true }) { id trader initiatedAt isBuy notional tradeNotional collateral leverage limitType openPrice stopLossPrice takeProfitPrice updatedAt pair { id from to } } orders(first: 1000, where: { trader: $trader, isPending: true, isCancelled: false, orderType: \"Market\" }) { id trader pair { id from to } } }"' \
    '{"trader": "0x98279066957a9eaf627d33983409a548cf3e2207"}'

# 15. GET_PAIRS
test_query "15. GET_PAIRS" \
    '"query GetPairsAndMetadata { pairs(first: 1000) { id from to } metaData(id: \"ostium\") { totalTrades totalUsers } }"' \
    ''

# 16. GET_SINGLE_PAIR
test_query "16. GET_SINGLE_PAIR" \
    '"query GetSinglePair($pairId: ID!) { pair(id: $pairId) { id from to } }"' \
    '{"pairId": "0"}'

# 17. GET_USER_TRADES
test_query "17. GET_USER_TRADES" \
    '"query getUserTrades($trader: Bytes!) { trades(where: { isOpen: true, trader: $trader }) { id tradeID trader pair { id from to } } }"' \
    '{"trader": "0x98279066957a9eaf627d33983409a548cf3e2207"}'

# 18. GET_LEADERBOARD_TRADES
test_query "18. GET_LEADERBOARD_TRADES" \
    '"query LeaderboardTrades { trades(where: { isOpen: true }) { id tradeID trader pair { id from to } } }"' \
    ''

# 19. GET_TRADES_DATA
test_query "19. GET_TRADES_DATA" \
    '"query GetTradesData($trader: Bytes!) { trades(where: { trader: $trader, isOpen: true }) { id tradeID trader pair { id from to } } orders(where: { trader: $trader, isPending: true, isCancelled: false, orderType: \"Market\" }) { id trader pair { id from to } } limits(where: { trader: $trader, isActive: true }) { id trader isActive initiatedAt isBuy notional tradeNotional collateral leverage limitType openPrice stopLossPrice takeProfitPrice updatedAt pair { id from to } } }"' \
    '{"trader": "0x98279066957a9eaf627d33983409a548cf3e2207"}'

# 20. GET_REFERRED_USERS
test_query "20. GET_REFERRED_USERS" \
    '"query ReferredUsers($addresses: [ID!]) { users(where: { id_in: $addresses }) { id totalVolume } }"' \
    '{"addresses": ["0x98279066957a9eaf627d33983409a548cf3e2207"]}'

# 21. GET_USER_VOLUME
test_query "21. GET_USER_VOLUME" \
    '"query getUserVolume($id: ID!) { user(id: $id) { totalVolume } }"' \
    '{"id": "0x98279066957a9eaf627d33983409a548cf3e2207"}'

# 22. GET_LEADERBOARD_TRADERS
test_query "22. GET_LEADERBOARD_TRADERS" \
    '"query LeaderboardTraders($skip: Int, $first: Int) { users(first: $first, skip: $skip, orderBy: totalPnL, orderDirection: desc, where: { totalPnL_not: 0 }) { id totalPnL } }"' \
    '{"skip": 0, "first": 10}'

# 23. GET_USER
test_query "23. GET_USER" \
    '"query getUserPortfolio($id: ID!) { user(id: $id) { id totalPnL } }"' \
    '{"id": "0x98279066957a9eaf627d33983409a548cf3e2207"}'

# 24. GET_USER_PAIR_STATS
test_query "24. GET_USER_PAIR_STATS" \
    '"query getUserPairsStatistics($user: String) { userPairStats(where: { user: $user }) { totalPnL totalOpenTrades totalTrades totalMarketOrders totalOpenLimitOrders totalCancelledOrders id totalVolume totalOpenVolume totalClosedVolume totalProfitTrades totalLossTrades pair { id from to } } }"' \
    '{"user": "0x98279066957a9eaf627d33983409a548cf3e2207"}'

# 25. GET_USER_GROUP_STATS
test_query "25. GET_USER_GROUP_STATS" \
    '"query getUserGroupStatistics($user: String) { userGroupStats(where: { user: $user }) { totalPnL totalClosedCollateral totalOpenTrades totalTrades totalMarketOrders totalOpenLimitOrders totalClosedVolume totalOpenVolume totalCancelledOrders id totalVolume totalProfitTrades totalLossTrades group { id name } } }"' \
    '{"user": "0x98279066957a9eaf627d33983409a548cf3e2207"}'

# 26. GET_PROTOCOL_VAULT_DATA
test_query "26. GET_PROTOCOL_VAULT_DATA" \
    '"query getProtocolData { vault(id: \"usdc-vault\") { id shares assets } }"' \
    ''

# ========================================
# Additional Queries (Set 2)
# ========================================

# 27. GET_OPEN_TRADES
test_query "27. GET_OPEN_TRADES" \
    '"query trades($skip: Int!, $pairId: String!) { trades(first: 10000, skip: $skip, where: { isOpen: true, pair: $pairId }) { tradeID collateral leverage highestLeverage openPrice stopLossPrice takeProfitPrice isOpen isBuy notional funding rollover trader index closeInitiated } }"' \
    '{"skip": 0, "pairId": "0"}'

# 28. GET_PAIR (Full Data)
test_query "28. GET_PAIR_FULL" \
    '"query pair($id: String!) { pair(id: $id) { id feed totalOpenTrades totalOpenLimitOrders accRollover lastRolloverBlock rolloverFeePerBlock accFundingLong accFundingShort longOI shortOI lastFundingBlock maxFundingFeePerBlock lastFundingRate maxOI hillInflectionPoint hillPosScale hillNegScale springFactor sFactorUpScaleP sFactorDownScaleP lastTradePrice maxLeverage overnightMaxLeverage netVolThreshold decayRate priceImpactK buyVolume sellVolume lastUpdateTimestamp isNegativeRolloverAllowed lastRolloverLongPure brokerPremium accRolloverLong accRolloverShort group { maxLeverage longCollateral shortCollateral maxCollateralP } } }"' \
    '{"id": "0"}'

# 29. GET_PAIR_FEED
test_query "29. GET_PAIR_FEED" \
    '"query pair($id: String!) { pair(id: $id) { feed } }"' \
    '{"id": "0"}'

# 30. GET_OPEN_LIMITS
test_query "30. GET_OPEN_LIMITS" \
    '"query limits($skip: Int!, $pairId: String!) { limits(first: 10000, skip: $skip, where: { isActive: true, pair: $pairId }) { id trader block uniqueId isBuy limitType openPrice leverage notional collateral executionStarted } }"' \
    '{"skip": 0, "pairId": "0"}'

# 31. GET_METADATA
test_query "31. GET_METADATA" \
    '"query metaDatas { metaDatas { id maxNegativePnlOnOpenP isTradingPaused isCallbackPaused } }"' \
    ''

# 32. GET_VAULT_ASSETS
test_query "32. GET_VAULT_ASSETS" \
    '"query vaults { vaults(where: { id: \"usdc-vault\" }) { assets } }"' \
    ''

# 33. GET_LPACTIONS
test_query "33. GET_LPACTIONS" \
    '"query lpActions($epoch: String!, $type: String!) { lpActions(where: { withdrawUnlockEpoch: $epoch, type: $type }) { user withdrawShares } }"' \
    '{"epoch": "1", "type": "WITHDRAW"}'

# 34. GET_VAULT_EPOCH
test_query "34. GET_VAULT_EPOCH" \
    '"query vault { vaults(where: { id: \"usdc-vault\" }) { currentEpoch currentEpochStart } }"' \
    ''

# 35. GET_USER_APPROVALS
test_query "35. GET_USER_APPROVALS" \
    '"query users($users: [String!]!) { users(where: { id_in: $users }) { id keeperAllowance } }"' \
    '{"users": ["0x98279066957a9eaf627d33983409a548cf3e2207"]}'

# 36. GET_ALL_PAIRS
test_query "36. GET_ALL_PAIRS" \
    '"query pairs { pairs { id } }"' \
    ''

# 37. GET_LP_ACTIONS_BETWEEN_TIMES
test_query "37. GET_LP_ACTIONS_BETWEEN_TIMES" \
    '"query GetLpActions($startTime: BigInt!, $endTime: BigInt!, $skip: Int = 0, $first: Int = 1000) { lpActions(where: { timestamp_gte: $startTime, timestamp_lte: $endTime }, orderBy: timestamp, orderDirection: asc, skip: $skip, first: $first) { id user assets shares type timestamp } }"' \
    '{"startTime": "0", "endTime": "9999999999", "skip": 0, "first": 100}'

# 38. GET_ORDERS_BETWEEN_TIMES
test_query "38. GET_ORDERS_BETWEEN_TIMES" \
    '"query GetOrders($startTime: BigInt!, $endTime: BigInt!, $skip: Int = 0, $first: Int = 1000) { orders(where: { executedAt_gte: $startTime, executedAt_lte: $endTime }, orderBy: executedAt, orderDirection: asc, skip: $skip, first: $first) { id trader notional executedAt initiatedAt isPending isCancelled pair { id group { name } } } }"' \
    '{"startTime": "0", "endTime": "9999999999", "skip": 0, "first": 100}'

# 39. GET_USERS_VOLUMES
test_query "39. GET_USERS_VOLUMES" \
    '"query GetAllTimeAllTradersVolumes($skip: Int = 0, $first: Int = 1000) { users(orderBy: totalVolume, orderDirection: desc, skip: $skip, first: $first) { id totalVolume } }"' \
    '{"skip": 0, "first": 100}'

echo ""
echo "========================================"
echo "Test Results"
echo "========================================"
echo -e "${GREEN}Passed: $PASSED${NC}"
echo -e "${RED}Failed: $FAILED${NC}"
echo "Total: $((PASSED + FAILED))"
echo ""

if [ $FAILED -gt 0 ]; then
    exit 1
else
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
fi

