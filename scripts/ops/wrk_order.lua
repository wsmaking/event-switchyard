-- wrk用 注文POSTスクリプト
-- 使用例: wrk -c50 -t4 -d30s -s scripts/ops/wrk_order.lua http://localhost:8091/api/orders

wrk.method = "POST"
wrk.headers["Content-Type"] = "application/json"

-- カウンタ（リクエストごとにaccountIdを変える）
local counter = 0

function request()
    counter = counter + 1
    local body = string.format(
        '{"accountId":%d,"symbol":"AAPL","side":"BUY","qty":100,"price":15000}',
        counter
    )
    return wrk.format(nil, nil, nil, body)
end

function response(status, headers, body)
    -- エラーレスポンスをカウント（オプション）
    if status ~= 200 then
        io.stderr:write(string.format("Error: %d - %s\n", status, body))
    end
end
