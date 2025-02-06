local overloaded<const> = {}
local objectoverloaded<const> = {}
local overloadedreferences<const> = {}
_G.Type = _G.type
local _t<const> = _G.type
function type(v)
    local _type = _t(v)
    if _type == "table" then
        local mt = getmetatable(v)
        if mt and mt.__type then
            return mt.__type
        end
        return _type
    end
    return _type
end
local _type<const> = _G.type

local function callable(func)
    local func_type = _t(func)
    if func_type == "function" then return true end
    if func_type == "table" then
        local mt = getmetatable(func)
        if mt and mt.__call then
            return true
        end
    end
    return false
end

function IsOverloaded(func)
    local func_type = type(func)

    if func_type:find("overloaded") then return true end
    if func_type == "function" then return overloadedreferences[func] ~= nil end
    if func_type == "table" then return overloadedreferences[func] ~= nil end
    return false
end

---Returns data associated with given overloaded reference
---If no reference is found, return false
---@param func function|table reference to overloaded function
---@return {[1]: table, [2]: function, [3]: number, [4]: string, [5]: string, [6]: function}|false result associated with reference or false
function GetOverloadedData(func)
    if not IsOverloaded(func) then return false end
    return overloadedreferences[func]
end

function ReplaceOverloadedFunction(func, replace)
    if not callable(replace) then return false end
    local data = GetOverloadedData(func)
    if not data then return false end
    data[2] = replace
    overloadedreferences[replace] = data
    return true
end

local function isarray(tbl)
    if #tbl == 0 then
        return next(tbl) == nil
    end
    return true
end

local function getTrailingNils(array)
    local result = 0
    for i=#array, 1, -1 do
        if _type(array[i]) == "table" then
            for j=1, #array[i] do
                if array[i][j] == "nil" then
                    result += 1
                    break
                end
            end
        elseif array[i] == "nil" then
            result += 1
        end
    end
    return result
end

---@param to_check table
---@param parameters table
---@param trailingNils number
---@return boolean
function areParameterTypesValid(to_check, parameters, trailingNils, name)
    local nToCheck = #to_check
    local nParameters = #parameters

    if nToCheck > nParameters then return false end
    if nParameters > nToCheck then
        if nParameters - nToCheck > trailingNils then
            return false
        end
    end
    for index, argument in ipairs(to_check) do
        local argumentType = _t(argument)
        if callable(argument) then argumentType = "function" end
        local expectedType = parameters[index]
        if _t(expectedType) ~= "table" then
            local prefix = expectedType:sub(1, 4)
            if prefix ~= "not:" then
                if expectedType == "array" then
                    if not isarray(argument) then
                        return false    
                    end
                elseif argumentType ~= expectedType then
                    return false
                end
            else
                expectedType = expectedType:sub(5, expectedType:len())
                if expectedType == "array" then
                    if isarray(argument) then
                        return false
                    end
                elseif argumentType == expectedType then
                    return false
                end
            end
        else
            local canpass = false
            for _, expectedTypeValue in ipairs(expectedType) do
                local prefix = expectedTypeValue:sub(1, 4)
                if prefix ~= "not:" then
                    if expectedTypeValue == "array" then
                        if isarray(argument) then
                            canpass = true
                            break
                        end
                    elseif argumentType == expectedTypeValue then
                        canpass = true
                        break
                    end
                else
                    expectedTypeValue = expectedTypeValue:sub(5, expectedTypeValue:len())
                    if expectedTypeValue == "array" then
                        if not isarray(argument) then
                            canpass = true
                            break
                        end
                    elseif argumentType ~= expectedTypeValue then
                        canpass = true
                        break
                    end
                end
            end
            if not canpass then
                return false
            end
        end
    end

    -- for k, param in ipairs(to_check) do
    --     local paramType = _t(param)
    --     if callable(param) then paramType = "function" end
    --     local expectedType = parameters[k]
    --     local expectedTypeTable = _t(expectedType) == "table"
    --     local not_type = false
    --     if not expectedTypeTable and expectedType:sub(1, 4) == "not:" then
    --         expectedType = expectedType:sub(5, expectedType:len())
    --         not_type = true
    --     end
    --     if expectedTypeTable == "table" then
    --         local canpass = false
    --         for _, v in ipairs(expectedType) do
    --             ---@diagnostic disable-next-line: redefined-local
    --             local not_type = v:sub(1, 4) == "not:"
    --             local f_type = not_type and v:sub(4, v:len()) or v
    --             print(not_type, f_type, paramType)
    --             if (not_type and f_type ~= paramType) or (not not_type and f_type == paramType) then
    --                 canpass = true
    --                 break
    --             end
    --         end
    --         if not canpass then return false end
    --     elseif not not_type and expectedType ~= paramType then
    --         print(paramType, expectedType, param, k, expectedTypeTable)
    --         if paramType == "table" then
    --             if expectedType == "function" then
    --                 local mt = getmetatable(param)
    --                 if not(mt and mt.__call or false) then
    --                     return false
    --                 end
    --             elseif expectedType == "array" then
    --                 if not isarray(param) then return false end
    --             end
    --         else
    --             return false
    --         end
    --     elseif not_type and expectedType == "array" then
    --         if paramType == "table" then
    --             if isarray(param) then
    --                 return false
    --             end
    --         end
    --     end
    --     return false
    -- end
    return true
end

-- local function concat(data)
--     local x = {}
--     for k,v in ipairs(data) do
--         x[k] = tostring(v)
--         if x[k]:find("function") then x[k] = "function" end
--         if x[k]:find("table") then x[k] = "table" end
--     end
--     return table.concat(x, " ")
-- end

---Overloads a function by overwriting the _G table, once a function is executed parameter types are
---Checked to see if given function is the one that is meant to be executed
---@param name string
---@param types string[]
---@param func function
local function _Overload(name, types, func)
    overloaded[name] = overloaded[name] or {}
    overloaded[name][#overloaded[name]+1] = {types, func, getTrailingNils(types), "function", name, nil}
    local overloadedtype = _type(_G[name])
    local metatable = (overloadedtype == "table" and getmetatable(_G[name]) or nil)
    if overloadedtype ~= "table" or (metatable and ((metatable.__type and metatable.__type:sub(1, 10) ~= "overloaded") or (not metatable.__call))) then
        _G[name] = setmetatable({}, {
            __call = function (_, ...)
                local args = {...}
                for _,v in ipairs(overloaded[name]) do
                    if (#args - v[3]) <= #v[1] and (v[6] or areParameterTypesValid)(args, v[1], v[3]) then
                        return v[2](...)
                    end
                end
                return error("Overloaded function '" .. name .. "' not found for given parameters.")
            end,
            __type = "overloaded:" .. name
        })
    end
    overloadedreferences[func] = overloaded[name][#overloaded[name]]
    return func
end

---Overloads a function by overwriting the _G table, once a function is executed parameter types are
---Checked to see if given function is the one that is meant to be executed
---@param name string
---@param types string[]
---@param func function
local function _OverloadExport(name, types, func)
    overloaded[name] = overloaded[name] or {}
    overloaded[name][#overloaded[name]+1] = {types, func, getTrailingNils(types), "function", name, nil}
    local overloadedtype = _type(_G[name])
    if overloadedtype ~= "function" then
        _G[name] = function (...)
            local args = {...}
            for _,v in ipairs(overloaded[name]) do
                if (#args - v[3]) <= #v[1] and (v[6] or areParameterTypesValid)(args, v[1], v[3]) then
                    return v[2](...)
                end
            end
            return error("Overloaded function '" .. name .. "' not found for given parameters.")
        end
    end
    overloadedreferences[func] = overloaded[name][#overloaded[name]]
    return func
end

local function _OverloadCustomTypeCheck(name, types, func, override)
    overloaded[name] = overloaded[name] or {}
    overloaded[name][#overloaded[name]+1] = {types, func, getTrailingNils(types), "function", name, override}
    local overloadedtype = _type(_G[name])
    if overloadedtype ~= "function" then
        _G[name] = function (...)
            local args = {...}
            for _,v in ipairs(overloaded[name]) do
                if (v[6] or areParameterTypesValid)(args, v[1], v[3]) then
                    return v[2](...)
                end
            end
            return error("Overloaded function '" .. name .. "' not found for given parameters.")
        end
    end
    overloadedreferences[func] = overloaded[name][#overloaded[name]]
    return func
end

local function _ObjectOverload(object, name, types, func)
    objectoverloaded[name] = objectoverloaded[name] or {}
    objectoverloaded[name][#objectoverloaded[name]+1] = {types, func, getTrailingNils(types), "object", name, nil}
    local overloadedtype = _type(object[name])
    if overloadedtype ~= "function" then
        -- Overloaded function that skips metatable due to how exports work
        object[name] = function(...)
            local args = {...}
            for _,v in ipairs(objectoverloaded[name]) do
                if (#args - v[3]) <= #v[1] and areParameterTypesValid(args, v[1], v[3], name) then
                    return v[2](...)
                end
            end
            return error("Overloaded function on object '" .. name .. "' not found for given parameters.")
        end
        overloadedreferences[func] = objectoverloaded[name][#objectoverloaded[name]]
    end
    return func
end

local function _ObjectOverloadCustomTypeCheck(object, name, func, override)
    objectoverloaded[object] = objectoverloaded[object] or {}
    objectoverloaded[object][name] = objectoverloaded[object][name] or {}
    objectoverloaded[object][name][#objectoverloaded[object][name]+1] = {{}, func, 0, "object", name, override}
    local overloadedtype = _type(object[name])
    if overloadedtype ~= "function" then
        -- Overloaded function that skips metatable due to how exports work
        object[name] = function(...)
            local args = {...}
            for _,v in ipairs(objectoverloaded[object][name]) do
                if v[6](args, v[1]) then
                    return v[2](...)
                end
            end
            return error("Overloaded function on object '" .. name .. "' not found for given parameters.")
        end
    end
    overloadedreferences[func] = objectoverloaded[object][name][#objectoverloaded[object][name]]
    return func
end

_Overload("Overload", {"string", "table", "function"}, _OverloadExport)
_Overload("Overload", {"string", "table", "function", "boolean"}, function(name, types, func, use_func)
    if use_func then
        return _Overload(name, types, func)
    end
    return _OverloadExport(name, types, func)
end)
_Overload("Overload", {"string", "table", "function", "function"}, function (name, types, func, override)
    return _OverloadCustomTypeCheck(name, types, func, override)
end)
_Overload("Overload", {"string", "function", "function"}, function (name, func, override)
    return _OverloadCustomTypeCheck(name, {}, func, override)
end)
_Overload("Overload", {"table", "string", "table", "function"}, function(object, name, types, func)
    return _ObjectOverload(object, name, types, func)
end)
_Overload("Overload", {"table", "string", "table", "function", "function"}, function(object, name, types, func, override)
    return _ObjectOverloadCustomTypeCheck(object, name, func, override)
end)
_Overload("Overload", {"table", "string", "function", "function"}, function(object, name, func, override)
    return _ObjectOverloadCustomTypeCheck(object, name, func, override)
end)

local objects<const> = {}
---@diagnostic disable-next-line: undefined-field
local mt = getmetatable(_G.Overload)
mt.__index = function(t, k)
    return setmetatable({}, {
        __index = function(_, name)
            return setmetatable({}, {
                __call = function(_, ...)
                    local object = _G[k] or objects[k]
                    if object == nil then
                        objects[k] = {}
                        object = objects[k]
                    end
                    --Overload object, two parameters are the object, and the name of the function, as for the rest just hope for good number of args
                    Overload(object, name, ...)
                end
            })
        end,
        __call = function(_, ...)
            --Good thing for overloading is that you don't have to worry about number of parameters, just use varargs and hope for the best
            Overload(t, ...)
        end
    })
end
---@diagnostic disable-next-line: undefined-field
setmetatable(_G.Overload, mt)

function Overload:PrepareLocalTable(name)
    objects[name] = {}
end

---@diagnostic disable-next-line: duplicate-set-field
function Overload:GetObject(name)
    return objects[name]
end

function Overload:init()
    ---@diagnostic disable-next-line: undefined-field
    local mt = getmetatable(_G.Overload)
    mt.__index = function(t, k)
        return setmetatable({}, {
            __index = function(_, name)
                return setmetatable({}, {
                    __call = function(_, ...)
                        local object = _G[k] or objects[k]
                        if object == nil then
                            objects[k] = {}
                            object = objects[k]
                        end

                        --Overload object, two parameters are the object, and the name of the function, as for the rest just hope for good number of args
                        Overload(object, name, ...)
                    end
                })
            end,
            __call = function(_, ...)
                --Good thing for overloading is that you don't have to worry about number of parameters, just use varargs and hope for the best
                Overload(t, ...)
            end
        })
    end
    ---@diagnostic disable-next-line: undefined-field
    setmetatable(_G.Overload, mt)
end
