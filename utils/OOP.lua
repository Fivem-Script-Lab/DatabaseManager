local overloaded<const> = {}
local objectoverloaded<const> = {}
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

local function getTrailingNils(array)
    local result = 0
    for i=#array, 1, -1 do
        if _type(array[i]) == "table" then
            for j=1, #array[i] do
                if array[i][j] == "table" then
                    if array[i][j] == "nil" then
                        result += 1
                        break
                    end
                end
            end
        elseif array[i] == "nil" then
            result += 1
        end
    end
    return result
end

local function areParameterTypesValid(to_check, parameters)
    local nToCheck = #to_check
    local nParameters = #parameters

    if nToCheck > nParameters then return false end

    if nParameters > nToCheck then
        for i = nToCheck + 1, nParameters do
            local param = parameters[i]
            if _type(param) == "table" then
                local canpass = false
                for _, v in ipairs(param) do
                    if v == "nil" then
                        canpass = true
                        break
                    end
                end
                if not canpass then return false end
            elseif param ~= "nil" then
                return false
            end
        end
    end

    for k, param in ipairs(to_check) do
        local paramType = _type(param)
        local expectedType = parameters[k]
        if _type(expectedType) == "table" then
            local canpass = false
            for _, v in ipairs(parameters[k]) do
                if v == paramType then
                    canpass = true
                    break
                end
            end
            if not canpass then return false end
        elseif expectedType ~= paramType then
            if expectedType == "function" and paramType == "table" then
                local mt = getmetatable(param)
                if not(mt and mt.__call or false) then
                    return false
                end
            else
                return false
            end
        end
    end

    return true
end

---Overloads a function by overwriting the _G table, once a function is executed parameter types are
---Checked to see if given function is the one that is meant to be executed
---@param name string
---@param types string[]
---@param func function
local function _Overload(name, types, func)
    overloaded[name] = overloaded[name] or {}
    overloaded[name][#overloaded[name]+1] = {types, func, getTrailingNils(types)}
    local overloadedtype = _type(_G[name])
    local metatable = (overloadedtype == "table" and getmetatable(_G[name]) or nil)
    if overloadedtype ~= "table" or (metatable and ((metatable.__type and metatable.__type:sub(1, 10) ~= "overloaded") or (not metatable.__call))) then
        _G[name] = setmetatable({}, {
            __call = function (t, ...)
                local args = {...}
                for _,v in ipairs(overloaded[name]) do
                    if (#args - v[3]) < #v[1] and areParameterTypesValid(args, v[1]) then
                        return v[2](...)
                    end
                end
                return error("Overloaded function '" .. name .. "' not found for given parameters.")
            end,
            __type = "overloaded:" .. name
        })
    end
end

---Overloads a function by overwriting the _G table, once a function is executed parameter types are
---Checked to see if given function is the one that is meant to be executed
---@param name string
---@param types string[]
---@param func function
local function _OverloadExport(name, types, func)
    overloaded[name] = overloaded[name] or {}
    overloaded[name][#overloaded[name]+1] = {types, func, getTrailingNils(types)}
    local overloadedtype = _type(_G[name])
    if overloadedtype ~= "function" then
        _G[name] = function (...)
            local args = {...}
            for _,v in ipairs(overloaded[name]) do
                if (#args - v[3]) < #v[1] and areParameterTypesValid(args, v[1]) then
                    return v[2](...)
                end
            end
            return error("Overloaded function '" .. name .. "' not found for given parameters.")
        end
    end
end

local function _OverloadCustomTypeCheck(name, types, func, override)
    overloaded[name] = overloaded[name] or {}
    overloaded[name][#overloaded[name]+1] = {types, func, getTrailingNils(types)}
    local overloadedtype = _type(_G[name])
    if overloadedtype ~= "function" then
        _G[name] = function (...)
            local args = {...}
            for _,v in ipairs(overloaded[name]) do
                if override(args, v[1]) then
                    return v[2](...)
                end
            end
            return error("Overloaded function '" .. name .. "' not found for given parameters.")
        end
    end
end

local function _ObjectOverload(object, name, types, func)
    objectoverloaded[name] = objectoverloaded[name] or {}
    objectoverloaded[name][#objectoverloaded[name]+1] = {types, func, getTrailingNils(types)}
    local overloadedtype = _type(object[name])
    if overloadedtype ~= "function" then
        -- Overloaded function that skips metatable due to how exports work
        object[name] = function(...)
            local args = {...}
            for _,v in ipairs(objectoverloaded[name]) do
                if (#args - v[3]) < #v[1] and areParameterTypesValid(args, v[1]) then
                    return v[2](...)
                end
            end
            return error("Overloaded function on object '" .. name .. "' not found for given parameters.")
        end
    end
end

local function _ObjectOverloadCustomTypeCheck(object, name, func, override)
    objectoverloaded[name] = objectoverloaded[name] or {}
    objectoverloaded[name][#objectoverloaded[name]+1] = {{}, func, 0}
    local overloadedtype = _type(object[name])
    if overloadedtype ~= "function" then
        -- Overloaded function that skips metatable due to how exports work
        object[name] = function(...)
            local args = {...}
            for _,v in ipairs(objectoverloaded[name]) do
                if override(args, v[1]) then
                    return v[2](...)
                end
            end
            return error("Overloaded function on object '" .. name .. "' not found for given parameters.")
        end
    end
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
_Overload("Overload", {"table", "string", "table", "function", "function"}, function(object, name, func, override)
    return _ObjectOverloadCustomTypeCheck(object, name, func, override)
end)
_Overload("Overload", {"table", "string", "function", "function"}, function(object, name, func, override)
    return _ObjectOverloadCustomTypeCheck(object, name, func, override)
end)

---@diagnostic disable-next-line: undefined-field
local mt = getmetatable(_G.Overload)
mt.__index = function(t, k)
    return setmetatable({}, {
        __index = function(_, name)
            return setmetatable({}, {
                __call = function(_, _, ...)
                    --Overload object, two parameters are the object, and the name of the function, as for the rest just hope for good number of args
                    Overload(_G[k], name, ...) 
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