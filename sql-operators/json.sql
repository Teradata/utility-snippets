--Parsing a column containing a JSON string
sel 
id
,NEW JSON(jsontext) jsondata            --Cast the string as JSON type
,jsondata..firstName firstName_extract  --Now we can use the dot notation to extract elements
,jsondata..age age_extract
,jsondata.JSONExtract('$..schools') schools_nested_array    --lists of elements can be extracted as an array
from
(
    sel 1 id, '[{"firstName" : "Cameron", "lastName" : "Lewis", "age" : 24, "schools" : [ {"name" : "Lake", "type" : "elementary"}, {"name" : "Madison", "type" : "middle"}, {"name" : "Rancho", "type" : "high"}, {"name" : "UCI", "type" : "college"} ], "job" : "programmer" }]' (VARCHAR(1000)) jsontext
) a;
 
---Shredding JSONs as a table
 
CREATE TABLE my_table (id INTEGER, jsonCol JSON(1000));
INSERT INTO my_table (1, NEW JSON('{"name" : "Cameron", "age" : 24,
"schools" : [ {"name" : "Lake", "type" : "elementary"},
{"name" : "Madison", "type" : "middle"}, {"name" : "Rancho", "type" : "high"},
{"name" : "UCI", "type" : "college"} ], "job" : "programmer" }'));
 
INSERT INTO my_table (2, NEW JSON('{"name" : "Melissa", "age" : 23,
"schools" : [ {"name" : "Lake", "type" : "elementary"},
{"name" : "Madison", "type" : "middle"},
{"name" : "Rancho", "type" : "high"},
{"name" : "Mira Costa", "type" : "college"} ] }'));
 
INSERT INTO my_table (3, NEW JSON('{"name" : "Alex", "age" : 25,
"schools" : [ {"name" : "Lake", "type" : "elementary"},
{"name" : "Madison", "type" : "middle"},
{"name" : "Rancho", "type" : "high"},
{"name" : "CSUSM", "type" : "college"} ], "job" : "CPA" }'));
 
INSERT INTO my_table (4, NEW JSON('{"name" : "David", "age" : 25,
"schools" : [ {"name" : "Lake", "type" : "elementary"},
{"name" : "Madison", "type" : "middle"},
{"name" : "Rancho", "type" : "high"} ],
"job" : "small business owner"}'));
 

SELECT * FROM JSON_Table
(ON (SELECT id, jsonCol FROM my_table)
USING rowexpr('$.schools[*]')
               colexpr('[ {"jsonpath" : "$.name",
                           "type" : "CHAR(20)"},
                          {"jsonpath" : "$.type",
                           "type" : "VARCHAR(20)"}]')
) AS JT(id, schoolName, "type");