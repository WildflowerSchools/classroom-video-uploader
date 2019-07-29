
if ARGV[1] then
    local key = ARGV[1]
    local keys = redis.call("HKEYS", key)
    local next_key = keys[1]
    local value = redis.call("HGET", key, next_key)
    redis.call("HDEL", key, next_key)
    redis.call("HSET", key .. ".active", next_key, value)
    return next_key
end

return ARGV
