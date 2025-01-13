DM.GetDatabaseName = function()
    local data = MySQL.query.await("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'")
    for i=1, #data do
        for k,v in pairs(data[i]) do
            if not v:match("BASE TABLE") then
                return k:gsub("Tables_in_", "")
            end
        end
    end
    return nil
end

DM.GetDatabaseTablesNames = function(dbname)
    local data = MySQL.query.await("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'")
    local tables = {}
    for _, sqltable in ipairs(data) do
        for k, v in pairs(sqltable) do
            if not v:match("BASE TABLE") then
                tables[#tables + 1] = v
            end
        end
    end
    return tables
end

DM.GetDatabaseTableColumns = function(dbname, tablename)
    local columns = MySQL.query.await(([[
        SELECT column_name FROM information_schema.columns 
        WHERE table_schema = '%s' AND table_name = '%s'
        ORDER BY table_name,ordinal_position
    ]]):format(dbname, tablename))
    local result = {}
    for _, column in ipairs(columns) do
        result[#result + 1] = column.column_name
    end
    return result
end

DM.GetAllDatabaseTablesColumns = function(dbname)
    local result = {}
    for _, sqltable in ipairs(DM.GetDatabaseTablesNames(dbname)) do
        local sqlt = DM.GetDatabaseTableColumns(dbname, sqltable)
        result[sqltable] = sqlt
    end
    return result
end

DM.GetDatabaseTablesColumnData = function(dbname, tablename)
    if not dbname then return {} end
    local columndata = MySQL.query.await(([[
        SELECT COLUMN_TYPE, EXTRA FROM information_schema.columns 
        WHERE table_schema = '%s' AND table_name = '%s'
        ORDER BY table_name,ordinal_position
    ]]):format(dbname, tablename))
    return {
        type = columndata[1]["COLUMN_TYPE"],
        extra = columndata[1]["EXTRA"]
    }
end

DM.GetAllDatabaseTablesColumnsExtra = function(dbname)
    local columns = DM.GetDatabaseTablesNames(dbname)
    local result = {}
    for _, sqltable in ipairs(columns) do
        result[sqltable] = DM.GetDatabaseTableColumnData(dbname, sqltable)
    end
    return result
end