Citizen.CreateThread(function()
    local resource = GetCurrentResourceName()
    local versionFile = LoadResourceFile(resource, '/version.json')
    if versionFile then
        print('^2version.json File correctly initialized!^7') 
        PerformHttpRequest("https://raw.githubusercontent.com/Fivem-Script-Lab/DatabaseManager/refs/heads/main/version.json", function (errorCode, resultData, resultHeaders)
            if errorCode == 200 then
                versionFile = string.gsub(versionFile, ' ', ''):gsub('["\n]', '')
                resultData = string.gsub(resultData, ' ', ''):gsub('["\n]', '')
                if versionFile == resultData then
                    print('^2You have up to date version of DatabaseManager!^7')
                else
                    print('^1You don\'t have the latest version of DatabaseManager!^7')
                    print('^3Current: ^1' .. versionFile .. ' ^3Latest: ^1' .. resultData .. '^7')
                end
            else
                print('^1Version check failed!^7')
            end
        end)
    else
        print('^1version.json not Found! Created new File!^7')
        PerformHttpRequest("https://raw.githubusercontent.com/Fivem-Script-Lab/DatabaseManager/refs/heads/main/version.json", function (errorCode, resultData, resultHeaders)
            if errorCode == 200 then
                SaveResourceFile(resource, '/version.json', tostring(resultData), -1)
            else
                SaveResourceFile(resource, '/version.json', '"0.0-X"', -1)
            end
        end)
    end
    Citizen.Wait(100)
end)