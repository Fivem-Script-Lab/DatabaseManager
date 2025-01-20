DM.SelectQuery = function(query, args, cb)
    return (cb and MySQL.prepare or MySQL.prepare.await)(query, args, cb)
end

DM.UpdateQuery = function(query, args, cb)
    return (cb and MySQL.prepare or MySQL.prepare.await)(query, args, cb)
end