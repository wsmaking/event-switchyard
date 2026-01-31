-- wrk script for POST /orders
-- Expects JWT_TOKEN to be set.

local path = os.getenv("WRK_PATH") or "/orders"
local token = os.getenv("JWT_TOKEN") or ""
local client_id_len = tonumber(os.getenv("CLIENT_ID_LEN") or "0")
local counter = 0

math.randomseed(os.time() + tonumber(tostring({}):sub(8), 16))

local body_template = '{"symbol":"AAPL","side":"BUY","type":"LIMIT","qty":100,"price":15000,"timeInForce":"GTC","clientOrderId":"%s"}'

wrk.method = "POST"
wrk.headers["Content-Type"] = "application/json"
if token ~= "" then
  wrk.headers["Authorization"] = "Bearer " .. token
end

request = function()
  counter = counter + 1
  local key = tostring(counter) .. "-" .. tostring(math.random(1, 1000000))
  if client_id_len > 0 then
    key = key .. string.rep("X", client_id_len)
  end
  local body = string.format(body_template, key)
  wrk.headers["Idempotency-Key"] = key
  return wrk.format(nil, path, nil, body)
end
