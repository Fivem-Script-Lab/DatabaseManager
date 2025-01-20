local _next = next
local _tbl_create = table.create
local _tbl_concat = table.concat

local function isTableEmpty(tbl)
    return _next(tbl) == nil
end

local function isarray(tbl)
    if #tbl == 0 then
        return _next(tbl) == nil
    else
        return true
    end
end

---@param tbl_name any
---@return string
local function createSelectAllQueryStart(tbl_name)
    return "SELECT * FROM `" .. tbl_name .. "`"
end

---@param arguments table
---@return string
---@return string[]
local function createSelectConditionsFromTable(arguments)
    local query = {}
    local args = {}
    for column, _ in pairs(arguments) do
        query[#query+1] = "`" .. column .. "` = ?"
        args[#args+1] = column
    end
    return _tbl_concat(query, " AND "), args
end

---@param arguments table
---@return string
---@return string[]
local function createSelectConditionsFromArray(arguments)
    local query = _tbl_create(#arguments, 0)
    for i=1, #arguments do
        query[#query+1] = "`" .. arguments[i] .. "` = ?"
    end
    return _tbl_concat(query, " AND "), arguments
end

---@param tbl_name string|nil
---@param arguments table|any[]|nil
---@param state number|nil
---@return string
---@return string[]|nil
function PrepareSelectStatement(tbl_name, arguments, state)
    state = state or 1
    arguments = arguments or {}
    local cond_func = (state ~= 2 and isarray(arguments)) and createSelectConditionsFromArray or createSelectConditionsFromTable

    if state == 1 then
        if isTableEmpty(arguments) then
            return createSelectAllQueryStart(tbl_name), nil
        end
        local cond_stmt, args = cond_func(arguments)
        return createSelectAllQueryStart(tbl_name) .. " WHERE " .. cond_stmt, args
    elseif state == 2 then
        return createSelectAllQueryStart(tbl_name)
    elseif state == 3 then
        return cond_func(arguments)
    end
    return "", nil
end

function PrepareSelectStatementConditionsIncludeNull(order, args)
    if isTableEmpty(args) then return "" end
    local result = _tbl_create(#order, 0)
    for i=1, #order do
        print(order[i], args[i])
        result[i] = order[i] .. " = " .. (args[i] == nil and "NULL" or "?")
    end
    return _tbl_concat(result, " AND ")
end