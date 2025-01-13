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

local function isarray(tbl)
    if #tbl == 0 then
        local _, t = pairs(tbl)
        return next(t) == nil
    else
        return true
    end
end

local _type = _G.type

local function callable(func)
    local func_type = _type(func)
    if func_type == "function" then return true end
    if func_type == "table" then
        local mt = getmetatable(func)
        if mt and mt.__call then
            return true
        end
    end
    return false
end

--- Retrieves a table manager for performing operations on a specific database table.
---
--- Provides a set of methods for database operations like `SELECT`, `INSERT`, `UPDATE`, `DELETE`,
--- as well as table-specific actions such as creating, dropping, or truncating the table.
---
--- @param table_name string The name of the database table to manage.
--- @return table # A table manager object with methods for various database operations.
exports("GetDatabaseTableManager", function(table_name)
    Overload:init()
    Overload:PrepareLocalTable("Prepare")
    
    Overload.Prepare.Select(
    --- Prepares a `SELECT` query with optional conditions.
    --- @param conditions table|nil Conditions for the query as column-value pairs.
    --- @param cb function|nil Optional callback for asynchronous execution.
    --- @param individual boolean|nil Whether to handle conditions individually or in bulk.
    --- @param query string|nil Additional SQL query string to append (e.g., `ORDER BY`).
    --- @return table # Prepared `SELECT` object with `execute` and `update` methods.
    function(conditions, cb, individual, query)
        local s_conditions, s_cb, s_cb_individual, s_query = conditions, cb, individual, query
        return {
            execute = function(conditions)
                return DM.SelectRows(table_name, conditions or s_conditions, s_cb, s_cb_individual, s_query)
            end,
            update = function(conditions, cb, individual, query)
                s_conditions, s_cb, s_cb_individual, s_query = table.unpack(
                    RequireNonNullValues(
                        {conditions, cb, individual, query},
                        {s_conditions, s_cb, s_cb_individual, s_query}
                    )
                )
            end
        }
    end, function(args)
        if not areParameterTypesValid(args, {
            {"nil", "table"}, {"nil", "function"}, {"nil", "boolean"}, {"nil", "string"}
        }, 4) then return false end
        if Type(args[1]) == "table" and isarray(args[1]) then return false end
        return true
    end)

    Overload.Prepare.Select(
    --- Prepares a `SELECT` query with optional conditions.
    --- @param fields string[] fields for the query from which values are prepared
    --- @param cb function|nil Optional callback for asynchronous execution.
    --- @param individual boolean|nil Whether to handle conditions individually or in bulk.
    --- @param query string|nil Additional SQL query string to append (e.g., `ORDER BY`).
    --- @return table # Prepared `SELECT` object with `execute` and `update` methods.
    function(fields, cb, individual, query)
        local s_sqlrow, s_fields, s_cb, s_cb_individual, s_query = nil, fields, cb, individual, query
        return {
            execute = function(...)
                s_sqlrow = {}
                local args = {...}
                for i=1, math.min(#s_fields, #args) do
                    s_sqlrow[s_fields[i]] = args[i]
                end
                return DM.SelectRows(table_name, s_sqlrow, s_cb, s_cb_individual, s_query)
            end,
            update = function(fields, cb, individual, query)
                s_fields, s_cb, s_cb_individual, s_query = table.unpack(
                    RequireNonNullValues(
                        {fields, cb, individual, query},
                        {s_fields, s_cb, s_cb_individual, s_query}
                    )
                )
            end
        }
    end, function(args)
        if not areParameterTypesValid(args, {
            {"table"}, {"nil", "function"}, {"nil", "boolean"}, {"nil", "string"}
        }, 3) then return false end
        if isarray(args[1]) then return true end
        return false
    end)

    Overload.Prepare.Select({"function"}, function(cb)
        local s_cb = cb
        return {
            execute = function()
                return DM.SelectRows(table_name, nil, s_cb, nil, nil)
            end,
            update = function(cb)
                s_cb = cb
            end
        }
    end)

    --- Prepares an `UPDATE` query for a single row.
    --- @param updates string[] A table of column names to update
    --- @param condition table A table of column-value pairs defining the condition.
    --- @param cb function|nil Optional callback for asynchronous execution.
    --- @return table # Prepared `UPDATE` object with `execute` and `update` methods.
    Overload.Prepare.Update(function(updates, condition, cb)
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
    end, function(args)
        if not areParameterTypesValid(args, {"table", "table", {"nil", "function"}}, 1) then
            return false
        end
        if not isarray(args[1]) or isarray(args[2]) then return false end
        return true
    end)

    --- Prepares an `UPDATE` query for a single row.
    --- @param updates_fields string[] A table of column names to update
    --- @param condition_fields string[] A table of column names defining the condition.
    --- @param cb function|nil Optional callback for asynchronous execution.
    --- @return table # Prepared `UPDATE` object with `execute` and `update` methods.
    Overload.Prepare.Update(function(updates_fields, condition_fields, cb)
        local s_sqlrow, s_sqlcond, s_updates, s_condition, s_cb = nil, nil, updates_fields, condition_fields, cb
        return {
            execute = function(updated, conditions)
                s_sqlrow = {}
                s_sqlcond = {}
                for i=1, math.min(#s_updates, #updated) do
                    s_sqlrow[s_updates[i]] = updated[i]
                end
                for i=1, math.min(#s_condition, #conditions) do
                    s_sqlcond[s_condition[i]] = conditions[i]
                end
                return DM.UpdateRow(table_name, s_sqlrow, s_sqlcond, s_cb)
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
    end, function(args)
        if not areParameterTypesValid(args, {"table", "table", {"nil", "function"}}, 1) then
            return false
        end
        if not isarray(args[1]) or not isarray(args[2]) then return false end
        return true
    end)

    local Prepare = Overload:GetObject("Prepare")

    --- Prepares an `UPDATE` query for multiple rows.
    --- @param updates table A table of column-value pairs to update.
    --- @param conditions table An array of conditions for the updates.
    --- @param cb function|nil Optional callback for asynchronous execution.
    --- @param individual boolean|nil Whether to handle updates individually.
    --- @param query string|nil Additional SQL query string to append.
    --- @return table # Prepared `UPDATE` object with `execute` and `update` methods.
    Prepare.UpdateRows = function(updates, conditions, cb, individual, query)
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
    end

    --- Prepares an `INSERT` query for a single row.
    --- @param row table Column names for the row.
    --- @param cb function|nil Optional callback for asynchronous execution.
    --- @return table # Prepared `INSERT` object with `execute` and `update` methods.
    Prepare.Insert = function(row, cb)
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
    end

    --- Prepares an `INSERT` query for multiple rows.
    --- @param fields string[] An array of column names for rows.
    --- @param cb function|nil Optional callback for asynchronous execution.
    --- @param individual boolean|nil Whether to handle inserts individually.
    --- @return table # Prepared `INSERT` object with `execute` and `update` methods.
    Prepare.InsertRows = function(fields, cb, individual)
        local s_fields, s_sqlrows, s_cb, s_individual = fields, {}, cb, individual
        return {
            execute = function(...)
                s_sqlrows = {}
                local args = {...}
                if #args == 0 then error("Invalid number of rows") end
                if _type(args[1][1]) == "table" then
                    local temp = {}
                    local next_i = 1
                    for i=1, #args do
                        for j=1, #args[i] do
                            if _type(args[i][j]) == "table" then
                                temp[next_i] = args[i][j]
                            else
                                temp[next_i] = args[i]
                            end
                            next_i += 1
                        end
                    end
                    args = temp
                end
                for i=1, #args do
                    local s_sqlrow = {}
                    for j=1, math.min(#s_fields, #args[i]) do
                        s_sqlrow[s_fields[j]] = args[i][j]
                    end
                    s_sqlrows[#s_sqlrows + 1] = s_sqlrow
                end
                return DM.InsertRows(table_name, s_sqlrows, s_cb, s_individual)
            end,
            update = function(fields, cb, individual)
                s_fields, s_cb, s_individual = table.unpack(
                    RequireNonNullValues(
                        {fields, cb, individual},
                        {s_fields, s_cb, s_individual}
                    )
                )
            end
        }
    end
    
    ---Prepares a 'DELETE' statement query for a single conditions
    ---@param row string[] column names
    ---@param cb? function|nil used for asynchronous deletion
    ---@return table # Prepared `DELETE` object with `execute` and `update` methods.
    Prepare.Delete = function(row, cb)
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
    end

    ---Prepares a 'DELETE' statement query for a single conditions
    ---@param rows table array of tables where each table contains data for each condition
    ---@param cb? function|nil used for asynchronous deletion
    ---@param individual? boolean|nil used for individual executions of each delete operation
    ---@return table # Prepared `DELETE` object with `execute` and `update` methods.
    Prepare.DeleteRows = function(rows, cb, individual)
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

    local data = {
        --- **Prepared Query Methods**
        Prepare = Prepare,
        --- **Table Operations**
        --- Creates a table with the specified structure.
        --- @param args table Arguments defining the table schema.
        --- @param truncate boolean whether to truncate the table if it exists
        --- @return boolean # Success or failure of the operation.
        Create = function(args, truncate)
            return DM.CreateTable(table_name, args, truncate)
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
            local args = {...}
            if callable(args[1]) then
                table.insert(args, 1, false)
            end
            return DM.SelectRows(table_name, table.unpack(args))
        end,
        Select = function(...)
            local args = {...}
            if callable(args[1]) then
                table.insert(args, 1, false)
            end
            return DM.Select(table_name, table.unpack(args))
        end,
        UpdateRows = function(...)
            return DM.UpdateRows(table_name, ...)
        end
    }

    return data
end)