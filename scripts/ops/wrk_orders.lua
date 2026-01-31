-- wrk script for POST /orders
-- Expects JWT_TOKEN to be set.

local path = os.getenv("WRK_PATH") or "/orders"
local token = os.getenv("JWT_TOKEN") or ""
local client_id_len = tonumber(os.getenv("CLIENT_ID_LEN") or "0")

local client_id = ""
if client_id_len > 0 then
  client_id = string.rep("X", client_id_len)
end

local body = string.format(
  '{"symbol":"AAPL","side":"BUY","type":"LIMIT","qty":100,"price":15000,"timeInForce":"GTC"%s}',
  client_id_len > 0 and string.format(',"clientOrderId":"%s"', client_id) or ""
)

wrk.method = "POST"
wrk.body = body
wrk.headers["Content-Type"] = "application/json"
if token ~= "" then
  wrk.headers["Authorization"] = "Bearer " .. token
end

request = function()
  return wrk.format(nil, path, nil, body)
end
