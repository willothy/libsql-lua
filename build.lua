local dir = vim.fn.fnamemodify(debug.getinfo(1, "S").source:sub(2), ":p:h")

vim.system({
	"cargo",
	"build",
	"--release",
}, {}):wait()

vim.system({
	"cp",
	"target/release/liblibsql.so",
	"lua/libsql/core.so",
}, {}):wait()
