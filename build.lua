local yield = coroutine.yield

local co
local resume = function(...)
	if co then
		coroutine.resume(co, ...)
	end
end

local function build()
	yield(vim.system({
		"cargo",
		"build",
		"--release",
	}, {}, resume))

	yield(vim.system({
		"cp",
		"target/release/liblibsql.so",
		"lua/libsql_core.so",
	}, {}, resume))
end

co = coroutine.create(build)
