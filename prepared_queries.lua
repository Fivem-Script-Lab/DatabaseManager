local _next = next
local _tbl_create = table.create
local _tbl_concat = table.concat
local _type = type

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
    for column, value in pairs(arguments) do
        local like_value = column:sub(1, 5) == "like:" or false
        if like_value then
            column = column:sub(6, column:len())
        elseif _type(value) == "string" then
            like_value = value:sub(1, 5) == "like:"
        end
        if like_value then
            query[#query+1] = "`" .. column .. "` LIKE ?"
        else
            query[#query+1] = "`" .. column .. "` = ?"
        end
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
        local like_value = arguments[i]:sub(1, 5) == "like:" or false
        if like_value then
            query[#query+1] = "`" .. arguments[i]:sub(6, arguments[i]:len()) .. "` LIKE ?"
        else
            query[#query+1] = "`" .. arguments[i] .. "` = ?"
        end
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
        local like_value = order[i]:sub(1,5) == "like:" or false
        if like_value then
            order[i] = order[i]:sub(6, order[i]:len())
        else
            like_value = _type(args[i]) == "string" and args[i]:sub(1, 5) == "like:"
            if like_value then
                args[i] = args[i]:sub(6, args[i]:len())
            end
        end
        result[i] = order[i] .. (like_value and " LIKE " or " = ") .. (args[i] == nil and "NULL" or "?")
    end
    return _tbl_concat(result, " AND ")
end