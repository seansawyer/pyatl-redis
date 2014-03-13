"""
Redis pipelines with asynchronous I/O.
"""
import redis, sha, time

# Lua script for tracking a click.
# TRACK key
#   1  now_ts
#   2  click_ts
#   3  click_url
TRACK = """local key = KEYS[1]
local now_ts = tonumber(ARGV[1])
local click_ts = tonumber(ARGV[2])
local click_url = ARGV[3]

local total_field = 'total_clicks'
local last_ts_field = 'last_click_ts'
local last_url_field = 'last_click_url'

local last_ts = tonumber(redis.call('HGET', key, last_ts_field))

-- Set the last click timestamp and URL if the click is new.
if last_ts == nil or click_ts > last_ts then
    last_ts = click_ts
    redis.call('HSET', key, last_ts_field, click_ts)
    redis.call('HSET', key, last_url_field, click_url)
end

-- Only hold onto hashes for a week after last click.
redis.call('EXPIREAT', key, last_ts + 604800)

-- Increment and return the total number of clicks.
return redis.call('HINCRBY', key, total_field, 1)"""
_sha = sha.new()
_sha.update(TRACK)
TRACK_SHA = _sha.hexdigest()


def load(r):
    results = r.script_exists(TRACK_SHA)
    if not results[0]:
        r.script_load(TRACK)


def track(r, key, click_ts, click_url):
    now = int(time.time())
    return r.evalsha(TRACK_SHA, 1, key, now, click_ts, click_url)


def main():
    r = redis.StrictRedis()
    load(r)
    now = int(time.time())
    track(r, 'user:sean', now - 60, 'https://alteregoapp.com/')
    track(r, 'user:sean', now - 3600, 'http://mandrill.com/')
    track(r, 'user:sean', now - 86400, 'http://mailchimp.com/')
    print r.hgetall('user:sean')
    print r.ttl('user:sean')

if __name__ == '__main__':
    main()
