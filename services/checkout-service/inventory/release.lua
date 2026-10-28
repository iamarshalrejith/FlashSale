
local inventory_key = KEYS[1]
local user_id = ARGV[1]

if not user_id or user_id == "" then
    return {0, "INVALID_USER"}
end

local user_key = inventory_key .. ":user:" .. user_id
local quantity = tonumber(redis.call("GET", user_key) or "0")

if quantity < 1 then
    return {0, "NO_RESERVATION"}
end

local remaining = redis.call("INCRBY", inventory_key, quantity)
redis.call("DEL", user_key)

return {1, tostring(remaining)}
