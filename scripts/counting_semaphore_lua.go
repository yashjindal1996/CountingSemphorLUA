package countingSemaphore

/*
This implements a counting semaphore referencing related algorithm in the book 'Redis in Action

SemKey : Key which is holding owner with timestamp
semLockKey : key which is holding zset with owner and key on which lock needs to be taken

No limit Semaphore:
	ARGV : limit = -1
	return : remaining = -1
*/

const ACQUIRE_LUA_SCRIPT = `
		local semKey        	= KEYS[1] -- ZSET holding owner with rank as timestamp.
		local semLockKey 		= KEYS[2] -- ZSET holding owner owners with lock key


		local now           = tonumber(ARGV[1])
		local limit         = tonumber(ARGV[2])
		local timeout       = tonumber(ARGV[3])
		local owner         = ARGV[4]
		local lockKey       = tonumber(ARGV[5])
		local allowed       = false

		redis.call('zremrangebyscore', semKey, '-inf', now - timeout)
		redis.call('zinterstore', semLockKey, 2, semLockKey, semKey, 'WEIGHTS', 1, 0)

		if limit == -1 then
			redis.call('zadd', semKey, now, owner)
			redis.call('zadd', semLockKey, lockKey, owner)
			return {allowed, -1}
		end

		local currentLockCount    = redis.call('zcount', semLockKey, lockKey, lockKey)

		if currentLockCount < limit then
			redis.call('zadd', semKey, now, owner)
			redis.call('zadd', semLockKey, lockKey, owner)
			allowed = true
		end

		local remaining = limit - currentLockCount + 1

		return {
			allowed,
			remaining
		}`

const RELEASE_LUA_SCRIPT = `
	local semKey        	= KEYS[1] -- ZSET holding owner with rank as timestamp.
	local semLockKey 		= KEYS[2] -- ZSET holding owner owners with lock key

	local owner         = ARGV[1]

	redis.call('zrem', semKey, owner)
	redis.call('zrem', semLockKey, owner)

	return {1}`

const GET_USED_LOCK_LUA_SCRIPT = `
		local semKey        	= KEYS[1] -- ZSET holding owner with rank as timestamp.
		local semLockKey 		= KEYS[2] -- ZSET holding owner owners with lock key

		local now           = tonumber(ARGV[1])
		local timeout       = tonumber(ARGV[2])
		local owner         = ARGV[3]
		local lockKey       = tonumber(ARGV[4])

		redis.call('zremrangebyscore', semKey, '-inf', now - timeout)
		redis.call('zinterstore', semLockKey, 2, semLockKey, semKey, 'WEIGHTS', 1, 0)

		local currentLockCount    = redis.call('zcard', semKey)
		return {currentLockCount}`

const REFRESH_LUA_SCRIPT = `
	local semKey        	= KEYS[1] -- ZSET holding owner with rank as timestamp.
	local semLockKey 		= KEYS[2] -- ZSET holding owner owners with lock key

	local now 			= ARGV[1]
	local owner 		= ARGV[2]
	local lockKey       = tonumber(ARGV[3])
	local refreshed 	= 1

	if redis.call('zadd', semKey, now, owner) ~= 0 then
		refreshed = 0
		redis.call('zrem', semKey, owner)
		redis.call('zrem', semLockKey, owner)
	end

	return {
		refreshed
	}`
