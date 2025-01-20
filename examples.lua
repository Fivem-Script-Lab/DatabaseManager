-- DM.CreateTableIfNotExists("job_grades_test", {
--     {"id", "INT(11) UNSIGNED PRIMARY KEY AUTO_INCREMENT"},
--     {"job_name", "VARCHAR(255) NOT NULL"},
--     {"job_label", "VARCHAR(255) NOT NULL"},
--     {"job_grade", "INT(11) UNSIGNED"},
--     {"paycheck", "INT(11) UNSIGNED DEFAULT 0"}
-- })
local job_grades = exports.DatabaseManager:GetDatabaseTableManager("job_grades_test")
job_grades.Create({
    {"id", "INT(11) UNSIGNED PRIMARY KEY AUTO_INCREMENT"},
    {"job_name", "VARCHAR(255) NOT NULL"},
    {"job_label", "VARCHAR(255) NOT NULL"},
    {"job_grade", "INT(11) UNSIGNED"},
    {"paycheck", "INT(11) UNSIGNED DEFAULT 0"}
})

local prepared_insert = job_grades.Prepare.Insert({
    "job_name", "job_label", "job_grade", "paycheck"
})

local prepared_multiple_inserts = job_grades.Prepare.InsertRows({
    "job_name", "job_label", "job_grade", "paycheck"
})

local prepared_select = job_grades.Prepare.Select({
    job_name = "police"
})

---- Or for ease of use
local prepared_dev_select = job_grades.Prepare.Select({"job_name"})

local created_grades = prepared_select.execute()
local created_dev_grades = prepared_dev_select.execute("police")
print(#(created_grades or {}))
print(#(created_dev_grades or {}))

prepared_insert.execute("police", "Sergeant", 2, 3000)
-- etc.

prepared_multiple_inserts.execute(
    {"police", "Recruit", 0, 1000},
    {"police", "Officer", 1, 2000}
)

created_grades = prepared_select.execute()
print(#created_grades)

local prepared_update = job_grades.Prepare.Update({
    "paycheck"
}, {
    job_name = "police"
})

----You can also simplify above function in a more developer friendly manner!

local prepared_dev_update = job_grades.Prepare.Update({
    "paycheck"
}, {
    "job_name"
})

prepared_update.execute(5000)
prepared_dev_update.execute({5000}, {"police"})
print(prepared_select.execute()[1].paycheck)
prepared_update.execute(2000)
prepared_dev_update.execute({2000}, {"police"})
print(prepared_select.execute()[1].paycheck)

local prepared_delete = job_grades.Prepare.Delete({
    "job_name"
})

prepared_delete.execute("police")
print(prepared_select.execute() == nil)