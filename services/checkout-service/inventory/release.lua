
local inventory_key = KEYS[1]
local order_id = ARGV[1]

if not order_id or order_id == "" then
    return {0, "INVALID_ORDER"}
end

local reservation_key = inventory_key .. ":reservation:" .. order_id
local quantity = tonumber(redis.call("GET", reservation_key) or "0")

if not quantity or quantity < 1 then
    return {0, "NO_RESERVATION"}
end

local remaining = redis.call("INCRBY", inventory_key, quantity)
redis.call("DEL", reservation_key)

return {1, tostring(remaining)}
