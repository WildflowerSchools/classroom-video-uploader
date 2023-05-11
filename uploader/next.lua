
if ARGV[1] then
    local hash = ARGV[1]
    local keys = redis.call("HKEYS", hash)
    local next_key = keys[1]
    local value = redis.call("HGET", hash, next_key)
    redis.call("HDEL", hash, next_key)
    redis.call("HSET", hash .. ".active", next_key, value)
    return next_key
end

return ARGV
