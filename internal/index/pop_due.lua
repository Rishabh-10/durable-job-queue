-- Move due job ids from a queue's scheduling sorted set to its ready list.
--
-- KEYS[1] sorted set of scheduled job ids, scored by run_at in unix ms
-- KEYS[2] ready list consumed by workers
-- ARGV[1] current time in unix ms
-- ARGV[2] maximum ids to move in one call
--
-- Read, remove and push happen inside one script so two dispatchers can never
-- hand the same id to two workers. Without atomicity the duplicate would still
-- be caught by the claim in PostgreSQL, but only after both workers had gone to
-- the trouble of fetching the job.

local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
if #due == 0 then
  return {}
end

-- unpack has an argument-count limit, so move in chunks rather than trusting
-- the batch size to stay small. 512 is well inside every Lua build's headroom.
local chunk = 512
for start = 1, #due, chunk do
  local stop = math.min(start + chunk - 1, #due)
  local slice = {}
  for n = start, stop do
    slice[#slice + 1] = due[n]
  end
  redis.call('ZREM', KEYS[1], unpack(slice))
  redis.call('RPUSH', KEYS[2], unpack(slice))
end

return due
