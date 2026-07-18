-- KEYS[1] = rate limit key (per user)
-- ARGV[1] = now, in milliseconds
-- ARGV[2] = window size, in milliseconds
-- ARGV[3] = max requests allowed per window
-- ARGV[4] = unique member id for this request (avoids ZADD collisions on same-ms requests)
--
-- Returns: { allowed (1/0), remaining, reset_after_millis }

local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local member = ARGV[4]

redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)
local count = redis.call('ZCARD', key)

if count < limit then
    redis.call('ZADD', key, now, member)
    redis.call('PEXPIRE', key, window)
    return { 1, limit - count - 1, window }
end

local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
local resetAfter = window
if oldest[2] ~= nil then
    resetAfter = (tonumber(oldest[2]) + window) - now
end

return { 0, 0, resetAfter }
