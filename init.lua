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