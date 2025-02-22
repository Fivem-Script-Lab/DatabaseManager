local _tbl_create = table.create
local _tbl_concat = table.concat
local _tbl_remove = table.remove
local _len = string.len
local _pairs = pairs
local _ipairs = ipairs

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
    for i,v in _pairs(default) do
        local value = tbl[i] or v
        if value == nil then value = false end
        result[i] = value
    end
    return result
end

local function isarray(tbl)
    if #tbl == 0 then
        return next(tbl) == nil
    end
    return true
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

local function getValuesInOrder(tbl_v, tbl_order)
    local values = {}
    for i=1, #tbl_order do
        values[i] = tbl_v[tbl_order[i]]
    end
    return values
end

local function getKeys(tbl)
    local keys = {}
    for k,_ in _pairs(tbl) do
        keys[#keys+1] = k
    end
    return keys
end

local _PrepareSelectStatement = PrepareSelectStatement
local _PrepareSelectStatementConditionsIncludeNull = PrepareSelectStatementConditionsIncludeNull
local _DM_SelectQuery = DM.SelectQuery

local _init = false

--- Retrieves a table manager for performing operations on a specific database table.
---
--- Provides a set of methods for database operations like `SELECT`, `INSERT`, `UPDATE`, `DELETE`,
--- as well as table-specific actions such as creating, dropping, or truncating the table.
---
--- @param table_name string The name of the database table to manage.
--- @return table # A table manager object with methods for various database operations.
exports("GetDatabaseTableManager", function(table_name)
    if _init == false then
        Overload:init()
        Overload:PrepareLocalTable("Prepare")

        Overload.Prepare.Select(
            {{"nil", "not:array"}, {"nil", "function"}, {"nil", "boolean"}, {"nil", "string"}},
        --- Prepares a `SELECT` query with optional conditions.
        --- @param conditions table|nil Conditions for the query as column-value pairs.
        --- @param cb function|nil Optional callback for asynchronous execution.
        --- @param individual boolean|nil Whether to handle conditions individually or in bulk.
        --- @param query string|nil Additional SQL query string to append (e.g., `ORDER BY`).
        --- @return table # Prepared `SELECT` object with `execute` and `update` methods.
        function(conditions, cb, individual, query)
            local s_conditions, s_cb, s_cb_individual, s_query = conditions or {}, cb, individual, query

            local prepared_query_start = _PrepareSelectStatement(table_name, nil, 2)
            local prepared_arguments, order = _PrepareSelectStatement(nil, conditions, 3)
            local query_parts = { prepared_query_start, _len(prepared_arguments) > 0 and "WHERE" or "", prepared_arguments }
            order = order or {}
            local arguments = getValuesInOrder(conditions, order)
            return {
                execute = function(new_conditions)
                    if new_conditions then
                        local new_keys = getKeys(new_conditions)
                        if #new_keys ~= #arguments then
                            prepared_arguments, order = _PrepareSelectStatement(table_name, new_conditions, 3)
                            if #order == 0 then query_parts[2] = "" end
                            arguments = getValuesInOrder(new_conditions, order)
                            query_parts[3] = prepared_arguments
                        else
                            for i=1, #new_keys do
                                if not s_conditions[new_keys[i]] then
                                    prepared_arguments, order = _PrepareSelectStatement(table_name, new_conditions, 3)
                                    if #order == 0 then query_parts[2] = "" end
                                    arguments = getValuesInOrder(new_conditions, order)
                                    query_parts[3] = prepared_arguments
                                    break
                                end
                            end
                        end
                    end
                    return _DM_SelectQuery(_tbl_concat(query_parts, " "), arguments, s_cb)
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
        end)

        Overload.Prepare.Select(
            {"array", {"nil", "function"}, {"nil", "boolean"}, {"nil", "string"}},
        --- Prepares a `SELECT` query with optional conditions.
        --- @param fields string[] fields for the query from which values are prepared
        --- @param cb function|nil Optional callback for asynchronous execution.
        --- @param individual boolean|nil Whether to handle conditions individually or in bulk.
        --- @param query string|nil Additional SQL query string to append (e.g., `ORDER BY`).
        --- @return table # Prepared `SELECT` object with `execute` and `update` methods.
        function(fields, cb, individual, query)
            local s_fields, s_cb, s_cb_individual, s_query = fields, cb, individual, query
            
            local prepared_query_start = _PrepareSelectStatement(table_name, nil, 2)
            local prepared_arguments = _PrepareSelectStatement(nil, fields, 3)
            local query_parts = { prepared_query_start, _len(prepared_arguments) > 0 and "WHERE" or "", prepared_arguments }

            local final_query = _tbl_concat(query_parts, " ")
            local fields_length = #fields
            return {
                execute = function(...)
                    local args = {...}
                    if fields_length ~= #args then
                        prepared_arguments = _PrepareSelectStatementConditionsIncludeNull(s_fields, args)
                        if #args == 0 then query_parts[2] = "" end
                        query_parts[3] = prepared_arguments
                        final_query = _tbl_concat(query_parts, " ")
                    else
                        for i=1, #s_fields do
                            if args[i] == nil or (_type(args[i]) == "string" and args[i]:sub(1, 5) == "like:") then
                                prepared_arguments = _PrepareSelectStatementConditionsIncludeNull(s_fields, args)
                                if #args == 0 then query_parts[2] = "" end
                                query_parts[3] = prepared_arguments
                                final_query = _tbl_concat(query_parts, " ")
                                for j=#args, 1, -1 do
                                    if args[j] == nil then
                                        _tbl_remove(args, j)
                                    end
                                end
                                break
                            end
                        end
                    end
                    return _DM_SelectQuery(final_query, args, s_cb)
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
        Overload.Prepare.Update({"array", "not:array", {"nil", "function"}}, function(updates, condition, cb)
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
        end)

        --- Prepares an `UPDATE` query for a single row.
        --- @param updates_fields string[] A table of column names to update
        --- @param condition_fields string[] A table of column names defining the condition.
        --- @param cb function|nil Optional callback for asynchronous execution.
        --- @return table # Prepared `UPDATE` object with `execute` and `update` methods.
        Overload.Prepare.Update({"array", "array", {"nil", "function"}}, function(updates_fields, condition_fields, cb)
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
        end)

        --- Prepares an `UPDATE` query for a single row.
        --- @param updates_fields string[] A table of column names to update
        --- @param condition_fields string[] A table of column names defining the condition.
        --- @param cb boolean|function if false, the callback is simply not provided
        --- @return table # Prepared `UPDATE` object with `execute` and `update` methods.
        Overload.Prepare.Update({"array", "array", {"function", "boolean"}}, function(updates_fields, condition_fields, cb)
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
                    if cb ~= false then
                        return DM.UpdateRow(table_name, s_sqlrow, s_sqlcond, s_cb)
                    else
                        return DM.UpdateRowNoCallback(table_name, s_sqlrow, s_sqlcond)
                    end
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
        end)
        _init = true
    end

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

    ORM = {}
    ORM.New = function(data, primary_fields, fields_hints)
        if _type(data) ~= "table" then return nil end
        local isMultiple = #data > 0
        if isMultiple then
            local results = _tbl_create(#data, 0)
            for i=1, #data do
                results[#results+1] = ORM.New(data[i], primary_fields)
            end
            return results
        end
        local object = {
            __data = {
                primary_fields = primary_fields,
                get_update_conditions = nil,
                auto_update = true
            },
            fields = {}
        }
        local updated_values = {}
        object.__data.get_update_conditions = function()
            local conditions = {}
            if not object.__data.primary_fields then
                for name, value in pairs(object.fields) do
                    if updated_values[name] == nil then
                        conditions[name] = value
                    end
                end
            else
                for _, name in ipairs(object.__data.primary_fields) do
                    conditions[name] = object.fields[name]
                end
            end
            return conditions
        end
        for field, value in pairs(data) do
            object.fields[field] = value
        end
        object.Set = function(name, value)
            object.fields[name] = value
            if object.__data.auto_update then
                local conditions = object.__data.get_update_conditions()
                if _type(value) == "table" then value = json.encode(value) end
                return DM.UpdateRow(table_name, {[name] = value}, conditions)
            else
                updated_values[name] = true
            end
        end
        object.Get = function(name)
            return object.fields[name]
        end
        object.AutoSave = function(auto)
            object.__data.auto_update = auto
        end
        object.Save = function()
            local conditions = object.__data.get_update_conditions()
            local fields = {}
            local updated = 0
            for name in pairs(updated_values) do
                updated += 1
                local value = object.fields[name]
                if _type(value) == "table" then value = json.encode(value) end
                fields[name] = value
            end
            if updated == 0 then return nil end
            return DM.UpdateRow(table_name, fields, conditions)
        end
        object.Fetch = function()
            if not object.__data.primary_fields then
                return print('[DatabaseManager:ERROR] ORM Object could not fetch data without Primary Keys defined')
            end
            local data = DM.SelectRows(table_name, object.__data.get_update_conditions())
            if not data then
                return print('[DatabaseManager:ERROR] ORM Object could not fetch data, Primary Keys not found')
            end
            if #data > 0 then data = data[1] end
            ---@diagnostic disable-next-line: param-type-mismatch
            for field, value in pairs(data) do
                local cur_value = value
                local field_hint = fields_hints[field]
                if field_hint then
                    if field_hint.json then
                        cur_value = json.decode(cur_value)
                    end
                    if field_hint.cb then
                        cur_value = field_hint.cb(cur_value)
                    end
                end
                object.fields[field] = value
            end
            updated_values = {}
        end
        return object
    end
    ORM.Define = function(primary_fields, fields_hints)
        local object = {}
        object.SelectSingle = function(conditions)
            local data = DM.SelectRows(table_name, conditions)
            if _type(data) ~= "table" then return {} end
            if data then
                if #data > 0 then
                    data = data[1]
                end
            end
            return ORM.New(data, primary_fields, fields_hints)
        end
        object.SelectAll = function(conditions)
            local data = DM.SelectRows(table_name, conditions)
            if not data then return {} end
            local result = ORM.New(data, primary_fields, fields_hints)
            if not result then return nil end
            if #result == 0 then result = { result } end
            return result
        end
        return object
    end

    local data = {
        __name = table_name,
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
        end,
        ORM = ORM
    }

    return data
end)