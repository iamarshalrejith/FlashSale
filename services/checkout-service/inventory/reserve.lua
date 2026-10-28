
local inventory_key = KEYS[1]
local quantity = tonumber(ARGV[1])
local order_id = ARGV[2]

if not quantity or quantity < 1 or quantity % 1 ~= 0 then
    return {0, "INVALID_QUANTITY"}
end

if not order_id or order_id == "" then
    return {0, "INVALID_ORDER"}
end

local reservation_key = inventory_key .. ":reservation:" .. order_id

if redis.call("EXISTS", reservation_key) == 1 then
    return {0, "ALREADY_RESERVED"}
end

local stock = tonumber(redis.call("GET", inventory_key) or "-1")

if not stock or stock < 0 then
    return {0, "INVENTORY_NOT_INITIALIZED"}
end

if stock < quantity then
    return {0, "SOLD_OUT"}
end

local remaining = redis.call("DECRBY", inventory_key, quantity)
redis.call("SET", reservation_key, quantity)

return {1, tostring(remaining)}
