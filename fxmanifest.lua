fx_version 'cerulean'

game 'gta5'

lua54 'yes'

author "Verbaz"
description "Provides basic wrappers for query creation"

server_scripts {
    '@mysql-async/lib/MySQL.lua',
    -- 'utils/OOP.lua',
    'server.lua',
    'database_dump.lua',
    'database_utils.lua',
    --"examples.lua"
}

provides "DatabaseManager"