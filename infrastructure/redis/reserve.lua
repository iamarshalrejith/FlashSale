local inventory_key = KEYS[1]
local quantity = tonumber(ARGV[1])
local user_id = ARGV[2]

if not quantity or quantity < 1 or quantity % 1 ~= 0 then
    return {0, "INVALID_QUANTITY"}
end

if not user_id or user_id == "" then
    return {0, "INVALID_USER"}
end

-- Prevent the same user from reserving twice.
local user_key = inventory_key .. ":user:" .. user_id

if redis.call("EXISTS", user_key) == 1 then
    return {0, "ALREADY_RESERVED"}
end

local stock = tonumber(redis.call("GET", inventory_key) or "-1")

if stock < 0 then
    return {0, "INVENTORY_NOT_INITIALIZED"}
end

if stock < quantity then
    return {0, "SOLD_OUT"}
end

local remaining = redis.call("DECRBY", inventory_key, quantity)

redis.call("SET", user_key, quantity)

return {1, tostring(remaining)}
