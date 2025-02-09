---@param table_name string table to be created it it already does not exist
---@param args string[][] an array of array of strings, each array represents a single column, the first index of each array is the column name
---@return boolean # true whether the table was created or it was not because it already existed
DM.CreateTable = function(table_name, args, truncate)
    if truncate and DM.DoesTableExist(table_name) then return DM.TruncateTable(table_name) end
    local data = {}
    for _, v in ipairs(args) do
        v[1] = '`' .. v[1] .. '`'
        data[#data + 1] = table.concat(v, " ")
    end
    MySQL.prepare.await(([[CREATE TABLE IF NOT EXISTS %s (%s)]]):format(table_name, table.concat(data, ", ")))
    return true
end

-- -@param table_name string table to be created it it already does not exist
-- -@param args string[][] an array of array of strings, each array represents a single column, the first index of each array is the column name
-- -@return boolean # true whether the table was created or it was not because it already existed
-- Overload.DM:CreateTable({"string", "table"}, function (table_name, args)
--     local data = {}
--     for _, v in ipairs(args) do
--         v[1] = '`' .. v[1] .. '`'
--         data[#data + 1] = table.concat(v, " ")
--     end
--     MySQL.prepare.await(([[CREATE TABLE IF NOT EXISTS %s (%s)]]):format(table_name, table.concat(data, ", ")))
--     return true
-- end)

-- -@param table_name string # table name
-- -@param args string[][] an array of array of strings, each array represents a single column, the first index of each array is the column name
-- -@return boolean #true if table was created or false if it already exists
-- Overload.DM:CreateTable({"string", "table", "boolean"}, function (table_name, args, truncate)
--     if truncate and DM.DoesTableExist(table_name) then return DM.TruncateTable(table_name) end
--     return DM.CreateTable(table_name, args)
-- end)

---@param table_name string table to be created it it already does not exist
---@return boolean # true whether the table was deleted or not
DM.DropTable = function(table_name)
    MySQL.prepare.await("DROP TABLE IF EXISTS " .. table_name)
    return true
end

---@param table_name string table to be created it it already does not exist
---@return boolean # if table does not exist then false, else true
DM.TruncateTable = function(table_name)
    if not DM.DoesTableExist(table_name) then return false end
    MySQL.prepare.await("TRUNCATE TABLE " .. table_name)
    return true
end

local cache_tables<const> = {}
---this function does not perform a query, it checks DM cache for existence of the table
---@param table_name string table to check if it exists
---@return boolean # true if table exists, false otherwise
DM.DoesTableExist = function(table_name)
    if cache_tables[table_name] then return true end
    if not DM.cache.ready then while not DM.cache.ready do Wait(10) end end
    for _, sqltable in ipairs(DM.cache.database_tables or {}) do
        if cache_tables[sqltable] == nil then cache_tables[sqltable] = true end
        if sqltable == table_name then return true end
    end
    return false
end
---clears local cache of DM.DoesTableExist
DM.ClearTableCache = function()
    for k in pairs(cache_tables) do
        cache_tables[k] = nil
    end
end

---performs additional check before creating a table
-- -@param table_name string # table name
-- -@param args string[][] an array of array of strings, each array represents a single column, the first index of each array is the column name
-- -@return boolean #true if table was created or false if it already exists
-- DM.CreateTableIfNotExists = function(table_name, args)
--     if DM.DoesTableExist(table_name) then return false end
--     return DM.CreateTable(table_name, args)
-- end

---creates a table if such does not exist, otherwise performs truncate operation
-- -@param args string[][] an array of array of strings, each array represents a single column, the first index of each array is the column name
-- -@return boolean # always true
-- DM.CreateTableIfNotExistsOrTruncate = function(table_name, args)
--     if not DM.DoesTableExist(table_name) then
--         return DM.CreateTable(table_name, args)
--     else
--         return DM.TruncateTable(table_name)
--     end
-- end

---returns all unique instances of keys in arrays provided
---@param tbl any[][] an array of values
---@return string[] #keys of any inner keys of objects
local function GetKeysFromInnerObjects(tbl)
    local keys = {}
    local found = {}
    for _,v in ipairs(tbl) do
        if type(v) == "table" then
            for k in pairs(v) do
                if not found[k] then
                    found[k] = true
                    keys[#keys + 1] = k
                end
            end
        end
    end
    return keys
end
---@param tbl table<string, any> object
---@return string[] #all keys of an object
local function GetKeys(tbl)
    local keys = {}
    for k in pairs(tbl) do keys[#keys + 1] = k end
    return keys
end
--- Returns values from a table in a specified order.
---
--- Creates a new table containing values from `tbl_v`, arranged according to keys
--- listed in `tbl_order`.
---
--- @param tbl_v table The source table containing values.
--- @param tbl_order table An array specifying the order of keys to extract from `tbl_v`.
--- @return any[] # A table of values from `tbl_v`, in the order of `tbl_order`.
local function GetValuesInOrder(tbl_v, tbl_order)
    local values = {}
    for _,k in ipairs(tbl_order) do
        values[#values + 1] = tbl_v[k]
    end
    return values
end
--- Checks if all values are present in given array
--- @param tbl any[] to check for values
--- @param values any[] values required in tbl
--- @return boolean #true if all values are present, otherwise false
local function ArrayContainsValues(tbl, values)
    for _, v in ipairs(values) do
        local contains = false
        for _, v2 in ipairs(tbl) do
            if v == v2 then
                contains = true
                break
            end
        end
        if not contains then return false end
    end
    return true
end

--- Inserts a single row into a database table.
---
--- Inserts the specified row into the table `table_name`. Supports both synchronous and asynchronous operations.
---
--- @param table_name string The name of the database table.
--- @param row table A table representing the row to insert (keys are column names).
--- @param cb? function|nil Optional callback function for asynchronous insertion.
--- @return boolean|nil #Returns `false` if the table doesn't exist. If synchronous, returns the result of the insertion.
DM.InsertRow = function(table_name, row, cb)
    if not DM.DoesTableExist(table_name) then return false end
    local keys = GetKeys(row)
    local values = GetValuesInOrder(row, keys)
    local value_placeholder = "(" .. string.rep("?,", #keys):sub(1, -2) .. ")"

    if cb then
        MySQL.insert(([[
            INSERT INTO %s (%s) VALUES(%s) %s
        ]]):format(table_name, table.unpack(keys), value_placeholder), values, cb)
    else
        return MySQL.insert.await(([[
            INSERT INTO %s (%s) VALUES %s
        ]]):format(table_name, table.concat(keys, ","), value_placeholder), values)
    end
end

--- Inserts multiple rows into a database table.
---
--- Inserts the rows into the table `table_name`. Supports inserting rows either individually or in bulk, 
--- and supports synchronous and asynchronous operations.
---
--- @param table_name string The name of the database table.
--- @param rows table[] An array of tables, where each table represents a row to insert.
--- @param cb? function|nil Optional callback function for asynchronous insertion.
--- @param individual? boolean If `true`, inserts each row individually; otherwise, inserts all rows in bulk.
--- @return table|boolean|nil # Returns a list of results for each row if synchronous; `false` if the table doesn't exist.
DM.InsertRows = function(table_name, rows, cb, individual)
    if not DM.DoesTableExist(table_name) then return false end

    local keys = GetKeysFromInnerObjects(rows)
    local fields = table.concat(keys, ",")
    local value_placeholders = "(" .. string.rep("?,", #keys):sub(1, -2) .. ")"
    local all_values = {}

    local values_list = {}
    for i = 1, #rows do
        local row = rows[i]
        local row_values = {}
        for j = 1, #keys do
            local value = row[keys[j]]
            if value == false then
                value = false
            elseif value == nil then
                value = nil
            end
            row_values[#row_values + 1] = value
        end
        values_list[#values_list + 1] = value_placeholders
        if not individual then
            for k=1, #row_values do
                all_values[#all_values + 1] = row_values[k]
            end
        else
            all_values[#all_values + 1] = row_values
        end
    end

    if individual then
        if cb then
            for i = 1, #values_list do
                local query = ([[INSERT INTO %s (%s) VALUES %s]]):format(table_name, fields, values_list[i])
                MySQL.insert(query, all_values[i], cb)
            end
        else
            local results = {}
            for i = 1, #values_list do
                local query = ([[INSERT INTO %s (%s) VALUES %s]]):format(table_name, fields, values_list[i])
                results[#results + 1] = MySQL.insert.await(query, all_values[i])
            end
            return results
        end
    else
        local query = ([[INSERT INTO %s (%s) VALUES %s]]):format(table_name, fields, table.concat(values_list, ","))
        if cb then
            MySQL.insert(query, all_values, cb)
        else
            return MySQL.insert.await(query, all_values)
        end
    end
end

--- Inserts multiple rows into a database table with optional `NULL` handling.
---
--- Similar to `InsertRows`, but ensures that `nil` values are treated as `'NULL'` in the database.
--- Supports both individual and bulk insertion, as well as synchronous and asynchronous operations.
---
--- @param table_name string The name of the database table.
--- @param rows table An array of tables, where each table represents a row to insert.
--- @param cb? function|nil Optional callback function for asynchronous insertion.
--- @param individual? boolean If `true`, inserts each row individually; otherwise, inserts all rows in bulk.
--- @return table|boolean|nil # Returns a list of results for each row if synchronous; `false` if the table doesn't exist.
DM.InsertRows2 = function(table_name, rows, cb, individual)
    if not DM.DoesTableExist(table_name) then return false end
    local keys = GetKeysFromInnerObjects(rows)
    local fields = table.concat(keys, ",")
    local value_placeholders = "(" .. string.rep("?,", #keys):sub(1, -2) .. ")"
    local all_values = {}

    for i = 1, #rows do
        local row = rows[i]
        local row_values = {}
        for j = 1, #keys do
            row_values[j] = row[keys[j]]
            if row_values[j] == nil then
                row_values[j] = 'NULL'
            end
        end

        if individual then
            all_values[#all_values + 1] = row_values
        else
            for j = 1, #row_values do
                all_values[#all_values + 1] = row_values[j]
            end
        end
    end

    local query = ([[INSERT INTO %s (%s) VALUES %s]]):format(table_name, fields, individual and value_placeholders or string.rep(value_placeholders .. ",", #rows):sub(1,-2))
    if individual then
        if cb then
            for i = 1, #rows do
                MySQL.prepare(query, all_values[i], cb)
            end
        else
            local results = {}
            for i = 1, #rows do
                results[#results + 1] = MySQL.prepare.await(query, all_values[i])
            end
            return results
        end
    else
        if cb then
            MySQL.prepare(query, all_values, cb)
        else
            return MySQL.prepare.await(query, all_values)
        end
    end
end

--- Deletes rows based on a single condition from a database table.
---
--- Constructs a `DELETE` query with conditions based on the specified `row` and executes it.
--- If there is no condition specified, the query will simply delete all rows
--- Supports both synchronous and asynchronous operations.
---
--- @param table_name string The name of the database table.
--- @param row? table|nil A table representing the conditions for deletion (keys are column names).
--- @param cb? function|nil Optional callback for asynchronous deletion.
--- @param query? string|nil Optional query string appended at the end of the query
--- @return boolean|nil # Returns `false` if the table doesn't exist. If synchronous, returns the result of the operation.
function DM.DeleteRow(table_name, row, cb, query)
    if not DM.DoesTableExist(table_name) then return false end
    query = query or ""
    local conditions = {}
    local values = {}
    
    if row then
        
        for column, value in pairs(row) do
            conditions[#conditions + 1] = column .. " = ?"
            values[#values + 1] = value
        end

        query = "DELETE FROM " .. table_name .. " WHERE " .. table.concat(conditions, " AND ") .. " " .. query
    else
        query = "DELETE FROM " .. table_name .. " " .. query
    end

    if cb then
        MySQL.prepare(query, values, cb)
    else
        return MySQL.prepare.await(query, values)
    end
end

--- Deletes multiple rows from a database table.
---
--- Constructs and executes `DELETE` queries for the specified rows. Supports deleting rows
--- individually or in bulk, as well as synchronous and asynchronous operations.
---
--- @param table_name string The name of the database table.
--- @param rows table An array of tables, where each table represents conditions for a row to delete.
--- @param cb? function|nil Optional callback for asynchronous deletion.
--- @param individual? boolean If `true`, deletes rows individually; otherwise, deletes all rows in a single query.
--- @return table|nil # Returns results for each query if synchronous; `false` if the table doesn't exist.
function DM.DeleteRows(table_name, rows, cb, individual)
    local conditions = {}
    local all_values = {}
    local key_lengths = {}

    for i = 1, #rows do
        local row = rows[i]
        local keys = GetKeys(row)
        key_lengths[i] = #keys
        local placeholders = {}
        for j = 1, #keys do
            placeholders[#placeholders + 1] = keys[j] .. " = ?"
            all_values[#all_values + 1] = row[keys[j]]
        end
        conditions[#conditions + 1] = "(" .. table.concat(placeholders, " AND ") .. ")"
    end

    if individual then
        local last_index = 0
        local function extract_row_values(index, length)
            return {table.unpack(all_values, index + 1, index + length)}
        end

        if cb then
            for i = 1, #conditions do
                local query = ([[DELETE FROM %s WHERE %s]]):format(table_name, conditions[i])
                local row_values = extract_row_values(last_index, key_lengths[i])
                last_index = last_index + key_lengths[i]
                MySQL.prepare(query, row_values, cb)
            end
        else
            local results = {}
            for i = 1, #conditions do
                local query = ([[DELETE FROM %s WHERE %s]]):format(table_name, conditions[i])
                local row_values = extract_row_values(last_index, key_lengths[i])
                last_index = last_index + key_lengths[i]
                results[#results + 1] = MySQL.prepare.await(query, row_values)
            end
            return results
        end
    else
        local query = ([[DELETE FROM %s WHERE %s]]):format(table_name, table.concat(conditions, " OR "))
        if cb then
            MySQL.prepare(query, all_values, cb)
        else
            return MySQL.prepare.await(query, all_values)
        end
    end
end

--- Selects rows from a database table based on conditions.
---
--- Constructs and executes `SELECT` queries for the specified conditions. Supports fetching rows 
--- individually or in bulk, as well as synchronous and asynchronous operations.
---
--- @param table_name string The name of the database table.
--- @param conditions? table|nil An array of tables, where each table specifies conditions for a query. If `nil`, selects all rows.
--- @param cb? function|nil Optional callback for asynchronous selection.
--- @param individual? boolean If `true`, executes a query for each set of conditions; otherwise, combines conditions into a single query.
--- @param query? string|nil Additional SQL query string to append (e.g., `LIMIT`, `ORDER BY`).
--- @return table|boolean|nil # Returns results for each query if synchronous; `false` if the table doesn't exist.
DM.SelectRows = function(table_name, conditions, cb, individual, query)
    if not DM.DoesTableExist(table_name) then return false end
    query = query or ""
    if not conditions then
        if cb then
            return MySQL.prepare("SELECT * FROM " .. table_name .. " " .. query, {}, cb)
        else
            return MySQL.prepare.await("SELECT * FROM " .. table_name .. " " .. query)
        end
    end
    if type(conditions) == "table" and #conditions == 0 then
        conditions = {conditions}
    end

    local query_conditions = {}
    local all_values = {}

    for i = 1, #conditions do
        ---@diagnostic disable-next-line: need-check-nil
        local condition = conditions[i]
        local keys = GetKeys(condition)
        local placeholders = {}

        for j = 1, #keys do
            placeholders[#placeholders + 1] = keys[j] .. " = ?"
            all_values[#all_values + 1] = condition[keys[j]]
        end

        query_conditions[#query_conditions + 1] = "(" .. table.concat(placeholders, " AND ") .. ")"
    end

    if individual then
        if cb then
            for i = 1, #query_conditions do
                query = ([[SELECT * FROM %s WHERE %s]]):format(table_name, query_conditions[i]) .. query
                ---@diagnostic disable-next-line: need-check-nil
                local keys_length = #GetKeys(conditions[i])
                local row_values = {table.unpack(all_values, (i - 1) * keys_length + 1, i * keys_length)}
                MySQL.prepare(query, row_values, cb)
            end
        else
            local results = {}
            for i = 1, #query_conditions do
                query = ([[SELECT * FROM %s WHERE %s]]):format(table_name, query_conditions[i]) .. query
                ---@diagnostic disable-next-line: need-check-nil
                local keys_length = #GetKeys(conditions[i])
                local row_values = {table.unpack(all_values, (i - 1) * keys_length + 1, i * keys_length)}
                results[#results + 1] = MySQL.prepare.await(query, row_values)
            end
            return results
        end
    else
        query = ([[SELECT * FROM %s WHERE %s]]):format(table_name, table.concat(query_conditions, " OR ")) .. query
        if cb then
            MySQL.prepare(query, all_values, cb)
        else
            return MySQL.prepare.await(query, all_values)
        end
    end
end

DM.Select = DM.SelectRows

--- Updates a single row in a database table.
---
--- Constructs an `UPDATE` query to modify values in `table_name` based on the specified `condition`.
--- Supports synchronous and asynchronous operations.
---
--- @param table_name string The name of the database table.
--- @param updates { [string]: any } A table of column-value pairs to update (keys are column names).
--- @param condition { [string]: any } A table of column-value pairs defining the condition for the update.
--- @param cb? function|boolean|nil Optional callback for asynchronous execution.
--- @param query? string|nil Additional SQL query string to append (e.g., `LIMIT`).
--- @return boolean|nil # Returns `false` if the table doesn't exist. If synchronous, returns the result of the operation.
DM.UpdateRow = function(table_name, updates, condition, cb, query)
    if not DM.DoesTableExist(table_name) then return false end
    query = query or ""
    local update_placeholders = {}
    local all_values = {}

    for column, value in pairs(updates) do
        update_placeholders[#update_placeholders + 1] = column .. " = ?"
        all_values[#all_values + 1] = value
    end

    local update_clause = table.concat(update_placeholders, ", ")

    local condition_placeholders = {}
    for column, value in pairs(condition) do
        condition_placeholders[#condition_placeholders + 1] = column .. " = ?"
        all_values[#all_values + 1] = value
    end

    local condition_clause = table.concat(condition_placeholders, " AND ")

    query = ([[UPDATE %s SET %s WHERE %s]]):format(table_name, update_clause, condition_clause) .. query

    if cb then
        MySQL.prepare(query, all_values, cb)
    else
        return MySQL.prepare.await(query, all_values)
    end
end

--- Updates a single row in a database table.
---
--- Constructs an `UPDATE` query to modify values in `table_name` based on the specified `condition`.
--- Supports synchronous and asynchronous operations.
---
--- @param table_name string The name of the database table.
--- @param updates { [string]: any } A table of column-value pairs to update (keys are column names).
--- @param condition { [string]: any } A table of column-value pairs defining the condition for the update.
--- @param query? string|nil Additional SQL query string to append (e.g., `LIMIT`).
--- @return boolean|nil # Returns `false` if the table doesn't exist. If synchronous, returns the result of the operation.
DM.UpdateRowNoCallback = function(table_name, updates, condition, query)
    if not DM.DoesTableExist(table_name) then return false end
    query = query or ""
    local update_placeholders = {}
    local all_values = {}

    for column, value in pairs(updates) do
        update_placeholders[#update_placeholders + 1] = column .. " = ?"
        all_values[#all_values + 1] = value
    end

    local update_clause = table.concat(update_placeholders, ", ")

    local condition_placeholders = {}
    for column, value in pairs(condition) do
        condition_placeholders[#condition_placeholders + 1] = column .. " = ?"
        all_values[#all_values + 1] = value
    end

    local condition_clause = table.concat(condition_placeholders, " AND ")

    query = ([[UPDATE %s SET %s WHERE %s]]):format(table_name, update_clause, condition_clause) .. query

    MySQL.prepare(query, all_values)
end

--- Updates multiple rows in a database table.
---
--- Constructs and executes `UPDATE` queries for the specified rows. Supports updating rows
--- individually or in bulk, as well as synchronous and asynchronous operations.
---
--- @param table_name string The name of the database table.
--- @param updates table A table of column-value pairs to update (keys are column names).
--- @param conditions { [string] : any }[] An array of tables, where each table specifies conditions for updating a row.
--- @param cb? function|nil Optional callback for asynchronous execution.
--- @param individual? boolean If `true`, updates rows individually; otherwise, combines conditions into a single query.
--- @param query? string|nil Additional SQL query string to append (e.g., `LIMIT`).
--- @return table|boolean|nil # Returns results for each query if synchronous; `false` if the table doesn't exist.
DM.UpdateRows = function(table_name, updates, conditions, cb, individual, query)
    if not DM.DoesTableExist(table_name) then return false end
    query = query or ""
    if #conditions == 0 then individual = false end
    local update_placeholders = {}
    local condition_placeholders = {}
    local all_values = {}

    for column, value in pairs(updates) do
        update_placeholders[#update_placeholders + 1] = column .. " = ?"
        all_values[#all_values + 1] = value
    end

    local update_clause = table.concat(update_placeholders, ", ")

    for i = 1, #conditions do
        local condition = conditions[i]
        local keys = GetKeys(condition)
        local placeholders = {}

        for j = 1, #keys do
            placeholders[#placeholders + 1] = keys[j] .. " = ?"
            all_values[#all_values + 1] = condition[keys[j]]
        end

        condition_placeholders[#condition_placeholders + 1] = "(" .. table.concat(placeholders, " AND ") .. ")"
    end

    if individual then
        local last_index = 0
        if cb then
            for i = 1, #condition_placeholders do
                local query = ([[UPDATE %s SET %s WHERE %s]]):format(table_name, update_clause, condition_placeholders[i]) .. query
                local keys_length = #GetKeys(conditions[i])
                local row_values = {table.unpack(all_values, last_index + 1, last_index + #updates + keys_length)}
                last_index = last_index + #updates + keys_length
                MySQL.prepare(query, row_values, cb)
            end
        else
            local results = {}
            for i = 1, #condition_placeholders do
                local query = ([[UPDATE %s SET %s WHERE %s]]):format(table_name, update_clause, condition_placeholders[i]) .. query
                local keys_length = #GetKeys(conditions[i])
                local row_values = {table.unpack(all_values, last_index + 1, last_index + #updates + keys_length)}
                last_index = last_index + #updates + keys_length
                results[#results + 1] = MySQL.prepare.await(query, row_values)
            end
            return results
        end
    else
        local query = ([[UPDATE %s SET %s WHERE %s]]):format(table_name, update_clause, table.concat(condition_placeholders, " OR ")) .. query
        if cb then
            MySQL.prepare(query, all_values, cb)
        else
            return MySQL.prepare.await(query, all_values)
        end
    end
end

DM.ready = true