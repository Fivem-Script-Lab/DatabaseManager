DM = {
    ready = false, -- value for methods of database readiness
    cache = {
        ready = false, -- value for cache value post-update
        database_name = nil,
        database_tables = nil,
        database_columns = nil
    }
}

CreateThread(function()
    while not DM.ready do Wait(0) end
    DM.cache.database_name = DM.GetDatabaseName()
    DM.cache.database_tables = DM.GetDatabaseTablesNames()
    DM.cache.database_columns = DM.GetAllDatabaseTablesColumns()
    DM.cache.ready = true
end)

DM.RefreshCache = function()
    DM.cache.database_name = DM.GetDatabaseName()
    DM.cache.database_tables = DM.GetDatabaseTablesNames()
    DM.cache.database_columns = DM.GetAllDatabaseTablesColumns()
end

exports("GetDatabaseManager", function()
    return DM
end)

--- Replaces `nil` values in a table with defaults or `false`.
---
--- Iterates through the `default` table (or `tbl` if `default` is not provided),
--- and creates a new table where `nil` values are replaced by corresponding values 
--- from `tbl` or `default`. If no replacement is found, uses `false`.
---
--- @param tbl table Input table to process.
--- @param default table|nil Optional table of default values. Defaults to `tbl` if not provided.
--- @return table # A new table with `nil` values replaced.
local function RequireNonNullValues(tbl, default)
    local result = {}
    default = default or tbl
    for i,v in ipairs(default) do
        local value = tbl[i] or v
        if value == nil then value = false end
        result[i] = value
    end
    return result
end

--- Retrieves a table manager for performing operations on a specific database table.
---
--- Provides a set of methods for database operations like `SELECT`, `INSERT`, `UPDATE`, `DELETE`,
--- as well as table-specific actions such as creating, dropping, or truncating the table.
---
--- @param table_name string The name of the database table to manage.
--- @return table # A table manager object with methods for various database operations.
exports("GetDatabaseTableManager", function(table_name)
    return {
        --- **Prepared Query Methods**
        Prepare = {
            --- Prepares a `SELECT` query with optional conditions.
            --- @param conditions table|nil Conditions for the query as column-value pairs.
            --- @param cb function|nil Optional callback for asynchronous execution.
            --- @param individual boolean|nil Whether to handle conditions individually or in bulk.
            --- @param query string|nil Additional SQL query string to append (e.g., `ORDER BY`).
            --- @return table # Prepared `SELECT` object with `execute` and `update` methods.
            Select = function(conditions, cb, individual, query)
                local s_conditions, s_cb, s_cb_individual, s_query = conditions, cb, individual, query
                return {
                    execute = function()
                        return DM.SelectRows(table_name, s_conditions, s_cb, s_cb_individual, s_query)
                    end,
                    update = function(sqlquery, conditions, cb, individual, query)
                        s_conditions, s_cb, s_cb_individual, s_query = table.unpack(
                            RequireNonNullValues(
                                {conditions, cb, individual, query},
                                {s_conditions, s_cb, s_cb_individual, s_query}
                            )
                        )
                    end
                }
            end,
            --- Prepares an `UPDATE` query for a single row.
            --- @param updates string[] A table of column names to update
            --- @param condition table A table of column-value pairs defining the condition.
            --- @param cb function|nil Optional callback for asynchronous execution.
            --- @return table # Prepared `UPDATE` object with `execute` and `update` methods.
            Update = function(updates, condition, cb)
                local s_sqlrow, s_updates, s_condition, s_cb = nil, updates, condition, cb
                return {
                    execute = function(...)
                        s_sqlrow = {}
                        local args = {...}
                        for i=1, math.min(#s_updates, #args) do
                            s_sqlrow[s_updates[i]] = args[i]
                        end
                        return DM.UpdateRow(table_name, s_sqlrow, s_condition, s_cb)
                    end,
                    update = function(updates, condition, cb)
                        s_updates, s_condition, s_cb = table.unpack(
                            RequireNonNullValues(
                                {updates, condition, cb},
                                {s_updates, s_condition, s_cb}
                            )
                        )
                    end
                }
            end,
            --- Prepares an `UPDATE` query for multiple rows.
            --- @param updates table A table of column-value pairs to update.
            --- @param conditions table An array of conditions for the updates.
            --- @param cb function|nil Optional callback for asynchronous execution.
            --- @param individual boolean|nil Whether to handle updates individually.
            --- @param query string|nil Additional SQL query string to append.
            --- @return table # Prepared `UPDATE` object with `execute` and `update` methods.
            UpdateRows = function(updates, conditions, cb, individual, query)
                local s_updates, s_conditions, s_cb, s_cb_individual, s_query = updates, conditions, cb, individual, query
                return {
                    execute = function(updates)
                        return DM.UpdateRows(table_name, updates or s_updates, s_conditions, s_cb, s_cb_individual, s_query)
                    end,
                    update = function(updates, conditions, cb, individual, query)
                        s_updates, s_conditions, s_cb, s_cb_individual, s_query = table.unpack(
                            RequireNonNullValues(
                                {updates, conditions, cb, individual, query},
                                {s_updates, s_conditions, s_cb, s_cb_individual, s_query}
                            )
                        )
                    end
                }
            end,
            --- Prepares an `INSERT` query for a single row.
            --- @param row table Column names for the row.
            --- @param cb function|nil Optional callback for asynchronous execution.
            --- @return table # Prepared `INSERT` object with `execute` and `update` methods.
            Insert = function(row, cb)
                local s_row, s_sqlrow, s_cb = row, nil, cb
                return {
                    execute = function(...)
                        s_sqlrow = {}
                        local args = {...}
                        for i=1, math.min(#s_row, #args) do
                            s_sqlrow[s_row[i]] = args[i]
                        end
                        return DM.InsertRow(table_name, s_sqlrow, s_cb)
                    end,
                    update = function(row, cb)
                        s_row, s_cb = table.unpack(
                            RequireNonNullValues(
                                {row, cb},
                                {s_row, s_cb}
                            )
                        )
                    end
                }
            end,
            --- Prepares an `INSERT` query for multiple rows.
            --- @param rows table An array of column names for rows.
            --- @param cb function|nil Optional callback for asynchronous execution.
            --- @param individual boolean|nil Whether to handle inserts individually.
            --- @return table # Prepared `INSERT` object with `execute` and `update` methods.
            InsertRows = function(rows, cb, individual)
                local s_rows, s_sqlrows, s_cb, s_individual = rows, {}, cb, individual
                return {
                    execute = function(...)
                        s_sqlrows = {}
                        local args = {...}
                        for i=1, #args do
                            local s_sqlrow = {}
                            for j=1, math.min(#s_rows, #args[i]) do
                                s_sqlrow[s_rows[j]] = args[i][j]
                            end
                            s_sqlrows[#s_sqlrows + 1] = s_sqlrow
                        end
                        return DM.InsertRows(table_name, s_sqlrows, s_cb, s_individual)
                    end,
                    update = function(rows, cb, individual)
                        s_rows, s_cb, s_individual = table.unpack(
                            RequireNonNullValues(
                                {rows, cb, individual},
                                {s_rows, s_cb, s_individual}
                            )
                        )
                    end
                }
            end,
            Delete = function(row, cb)
                local s_row, s_sqlrow, s_cb = row, {}, cb
                return {
                    execute = function(...)
                        s_sqlrow = {}
                        local args = {...}
                        for i=1, math.min(#s_row, #args) do
                            s_sqlrow[s_row[i]] = args[i]
                        end
                        return DM.DeleteRow(table_name, s_sqlrow, s_cb)
                    end,
                    update = function(row, cb)
                        s_row, s_cb = table.unpack(
                            RequireNonNullValues(
                                {row, cb},
                                {s_row, s_cb}
                            )
                        )
                    end
                }
            end,
            DeleteRows = function(rows, cb, individual)
                local s_rows, s_sqlrows, s_cb, s_individual = rows, {}, cb, individual
                return {
                    execute = function(...)
                        s_sqlrows = {}
                        local args = {...}
                        for i=1, #args do
                            local s_sqlrow = {}
                            for j=1, math.min(#s_rows, #args[i]) do
                                s_sqlrow[s_rows[j]] = args[i][j]
                            end
                            s_sqlrows[#s_sqlrows + 1] = s_sqlrow
                        end
                        return DM.DeleteRows(table_name, s_sqlrows, s_cb, s_individual)
                    end,
                    update = function (rows, cb, individual)
                        s_rows, s_cb, s_individual = table.unpack(
                            RequireNonNullValues(
                                {rows, cb, individual},
                                {s_rows, s_cb, s_individual}
                            )
                        )
                    end
                }
            end
        },
        --- **Table Operations**
        --- Creates a table with the specified structure.
        --- @param ... any Arguments defining the table schema.
        --- @return boolean # Success or failure of the operation.
        Create = function(...)
            return DM.CreateTable(table_name, ...)
        end,
        --- Drops the table from the database.
        --- @return boolean # Success or failure of the operation.
        Drop = function()
            return DM.DropTable(table_name)
        end,
        --- Removes all rows from the table without removing the table itself.
        --- @return boolean # Success or failure of the operation.
        Truncate = function()
            return DM.TruncateTable(table_name)
        end,
        --- Replaces the table by dropping and recreating it with a new schema.
        --- @param ... any Arguments defining the new table schema.
        --- @return boolean # Success or failure of the operation.
        Replace = function(...)
            DM.DropTable(table_name)
            return DM.CreateTable(table_name, ...)
        end,

        --- **Direct Query Methods**

        InsertRow = function(...)
            return DM.InsertRow(table_name, ...)
        end,
        InsertRows = function(...)
            return DM.InsertRows(table_name, ...)
        end,
        InsertRows2 = function(...)
            return DM.InsertRows2(table_name, ...)
        end,
        DeleteRow = function(...)
            return DM.DeleteRow(table_name, ...)
        end,
        DeleteRows = function(...)
            return DM.DeleteRows(table_name, ...)
        end,
        SelectRows = function(...)
            return DM.SelectRows(table_name, ...)
        end,
        UpdateRows = function(...)
            return DM.UpdateRows(table_name, ...)
        end
    }
end)