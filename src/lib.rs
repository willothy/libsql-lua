use std::{
    cell::Cell,
    ops::{Deref, DerefMut},
};

use futures::executor::block_on;
use mlua::{IntoLua, UserData, UserDataMethods};

struct Ser<T>(T);

impl<'lua> IntoLua<'lua> for Ser<libsql::Value> {
    fn into_lua(self, lua: &mlua::Lua) -> mlua::Result<mlua::Value> {
        use libsql::Value;
        match self.0 {
            Value::Null => Ok(mlua::Value::Nil),
            Value::Integer(i) => Ok(i.into_lua(lua)?),
            Value::Real(r) => Ok(r.into_lua(lua)?),
            Value::Text(s) => Ok(s.into_lua(lua)?),
            Value::Blob(b) => Ok(b.into_lua(lua)?),
        }
    }
}

pub struct Transaction(Option<Cell<libsql::Transaction>>);

impl Deref for Transaction {
    type Target = libsql::Transaction;
    fn deref(&self) -> &Self::Target {
        unsafe {
            Cell::as_ptr(&self.0.as_ref().expect("some"))
                .as_ref()
                .expect("non-null")
        }
    }
}

impl DerefMut for Transaction {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            Cell::as_ptr(&self.0.as_mut().expect("some"))
                .as_mut()
                .expect("non-null")
        }
    }
}

impl UserData for Transaction {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_method("execute", |_, tx, (sql, params): (String, Vec<String>)| {
            block_on(tx.execute(&*sql, params)).map_err(mlua::Error::external)
        });

        methods.add_method("execute_batch", |_, tx, sql: String| {
            block_on(tx.execute_batch(&*sql)).map_err(mlua::Error::external)
        });

        methods.add_method("is_autocommit", |_, tx, ()| Ok(tx.is_autocommit()));

        methods.add_method("query", |_, tx, (sql, params): (String, Vec<String>)| {
            block_on(tx.query(&*sql, params))
                .map(Rows)
                .map_err(mlua::Error::external)
        });

        methods.add_method_mut("commit", |_, tx, ()| {
            block_on(
                tx.0.take()
                    .ok_or_else(|| mlua::Error::external("Transaction already committed"))?
                    .into_inner()
                    .commit(),
            )
            .map_err(mlua::Error::external)
        });

        methods.add_method_mut("rollback", |_, tx, ()| {
            block_on(
                tx.0.take()
                    .ok_or_else(|| mlua::Error::external("Transaction already committed"))?
                    .into_inner()
                    .rollback(),
            )
            .map_err(mlua::Error::external)
        });

        methods.add_method("changes", |_, tx, ()| Ok(tx.changes()));

        methods.add_method("last_insert_rowid", |_, tx, ()| Ok(tx.last_insert_rowid()));
    }
}

pub struct Row(libsql::Row, i32);

impl Deref for Row {
    type Target = libsql::Row;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Row {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl UserData for Row {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_meta_method("__tostring", |_, row, ()| {
            let fields = (0..row.1)
                .map(|idx| {
                    let value = row.get_value(idx).expect("value");
                    let name = row.column_name(idx).expect("column name");
                    format!(
                        "{}: {}",
                        name,
                        match value {
                            libsql::Value::Null => "null".to_owned(),
                            libsql::Value::Integer(i) => i.to_string(),
                            libsql::Value::Real(f) => f.to_string(),
                            libsql::Value::Text(s) => s.to_string(),
                            libsql::Value::Blob(b) => String::from_utf8_lossy(&b).to_string(),
                        }
                    )
                })
                .collect::<Vec<_>>()
                .join(",\n");

            Ok(format!(
                "Row {{
                    {fields}
                }}"
            ))
        });

        methods.add_method("get", |_lua, row, i: i32| {
            row.get_value(i).map(Ser).map_err(mlua::Error::external)
        });

        methods.add_method("column_name", |_lua, row, i: i32| {
            Ok(row.column_name(i).map(|s| s.to_owned()))
        });

        methods.add_method("column_type", |_lua, row, i: i32| {
            row.column_type(i)
                .map(|t| match t {
                    libsql::ValueType::Integer => "integer",
                    libsql::ValueType::Real => "real",
                    libsql::ValueType::Text => "text",
                    libsql::ValueType::Blob => "blob",
                    libsql::ValueType::Null => "null",
                })
                .map_err(mlua::Error::external)
        });

        methods.add_method("column_count", |_lua, row, ()| Ok(row.1));

        methods.add_method("into_table", |lua, row, ()| {
            let table = lua.create_table()?;

            for idx in 0..row.1 {
                table.set(
                    row.column_name(idx).expect("column name"),
                    Ser(row.get_value(idx).map_err(mlua::Error::external)?),
                )?;
            }

            let fields = (0..row.1).map(|idx| {
                (
                    row.column_name(idx).expect("column name"),
                    Ser(row.get_value(idx).expect("column value")),
                )
            });

            Ok(lua.create_table_from(fields))
        })
    }
}

pub struct Rows(libsql::Rows);

impl Deref for Rows {
    type Target = libsql::Rows;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Rows {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl UserData for Rows {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_method_mut("next", |_, rows, ()| {
            Ok(block_on(rows.next())
                .map_err(mlua::Error::external)?
                .map(|r| Row(r, rows.column_count())))
        });

        methods.add_method("column_count", |_, rows, ()| Ok(rows.column_count()));

        methods.add_method("column_name", |_, rows, i: i32| {
            Ok(rows.column_name(i).map(|s| s.to_owned()))
        });

        methods.add_method("column_type", |_, rows, i: i32| {
            rows.column_type(i)
                .map(|t| match t {
                    libsql::ValueType::Integer => "integer",
                    libsql::ValueType::Real => "real",
                    libsql::ValueType::Text => "text",
                    libsql::ValueType::Blob => "blob",
                    libsql::ValueType::Null => "null",
                })
                .map_err(mlua::Error::external)
        });
    }
}

pub struct Connection(libsql::Connection);

impl Deref for Connection {
    type Target = libsql::Connection;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Connection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl UserData for Connection {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_method(
            "execute",
            |_, conn, (sql, params): (String, Vec<String>)| {
                block_on(conn.execute(&*sql, params)).map_err(mlua::Error::external)
            },
        );

        methods.add_method("query", |_, conn, (sql, params): (String, Vec<String>)| {
            block_on(conn.query(&*sql, params))
                .map(Rows)
                .map_err(mlua::Error::external)
        });

        methods.add_method("last_insert_rowid", |_, conn, ()| {
            Ok(conn.last_insert_rowid())
        });

        methods.add_method("changes", |_, conn, ()| Ok(conn.changes()));

        methods.add_method("transaction", |_, conn, ()| {
            Ok(Transaction(Some(Cell::new(
                block_on(conn.transaction()).map_err(mlua::Error::external)?,
            ))))
        });
    }
}

pub struct Database(libsql::Database);

impl Deref for Database {
    type Target = libsql::Database;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Database {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl UserData for Database {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_method("connect", |_, db, _: ()| {
            Ok(Connection(db.connect().map_err(mlua::Error::external)?))
        });
    }
}

fn open_in_memory(_lua: &mlua::Lua, _: ()) -> mlua::Result<Database> {
    let init = libsql::Builder::new_local(":memory:").build();
    let db = block_on(init).map_err(mlua::Error::external)?;
    Ok(Database(db))
}

fn open_file(_lua: &mlua::Lua, path: String) -> mlua::Result<Database> {
    let init = libsql::Builder::new_local(path).build();
    let db = block_on(init).map_err(mlua::Error::external)?;
    Ok(Database(db))
}

fn open_remote(_lua: &mlua::Lua, (url, token): (String, String)) -> mlua::Result<Database> {
    let init = libsql::Builder::new_remote(url, token).build();
    let db = block_on(init).map_err(mlua::Error::external)?;
    Ok(Database(db))
}

#[mlua::lua_module]
fn libsql_core(lua: &mlua::Lua) -> mlua::Result<mlua::Value> {
    let module = lua.create_table()?;

    module.set("open_in_memory", mlua::Function::wrap(open_in_memory))?;
    module.set("open", mlua::Function::wrap(open_file))?;
    module.set("open_remote", mlua::Function::wrap(open_remote))?;

    Err(mlua::Error::external("Not implemented"))
}
