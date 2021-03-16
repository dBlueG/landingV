f14 = LOAD '/user/cloudera/Volve/Logs/EnterpriseData/HIVE/F14.txt' 
   USING PigStorage('\t')
   as ( log_type:chararray,
   field:chararray,  
   well_name:chararray,  
   log_date:datetime, 
   well_condition:chararray, 
   depth:float, 
   keywords:chararray, 
   keywords_value:float ); 
   
f12 = LOAD '/user/cloudera/Volve/Logs/EnterpriseData/HIVE/F12.txt' 
   USING PigStorage('\t')
   as ( log_type:chararray,
   field:chararray,  
   well_name:chararray,  
   log_date:datetime, 
   well_condition:chararray, 
   depth:float, 
   keywords:chararray, 
   keywords_value:float ); 
   
f11B = LOAD '/user/cloudera/Volve/Logs/EnterpriseData/HIVE/F11B.txt' 
   USING PigStorage('\t')
   as ( log_type:chararray,
   field:chararray,  
   well_name:chararray,  
   log_date:datetime, 
   well_condition:chararray, 
   depth:float, 
   keywords:chararray, 
   keywords_value:float ); 

f1C = LOAD '/user/cloudera/Volve/Logs/EnterpriseData/HIVE/F1C.txt' 
   USING PigStorage('\t')
   as ( log_type:chararray,
   field:chararray,  
   well_name:chararray,  
   log_date:datetime, 
   well_condition:chararray, 
   depth:float, 
   keywords:chararray, 
   keywords_value:float );


all_wells = UNION f14, f12, f11B, f1C;


Filter_data = FILTER all_wells BY (keywords == 'QWZI' OR keywords == 'QWZT' OR keywords == 'QOZT' OR keywords == 'QOZI' OR keywords == 'QOIL');   
SPLIT Filter_data INTO withOil IF keywords_value > 1 , withoutOil IF keywords_value <= 0; 
Group_multiple = GROUP withOil by (well_name,log_date,well_condition, keywords); 
Summary  = FOREACH Group_multiple GENERATE group,  AVG(withOil.keywords_value) AS avg, COUNT(withOil.keywords_value) AS count , MIN(withOil.keywords_value) AS min ,MAX(withOil.keywords_value) AS max ;
sorted = ORDER Summary BY avg DESC; 
top5 = LIMIT sorted 5; 
dump sorted;
dump top5;


STORE Summary INTO '/user/cloudera/Volve/Logs/EnterpriseData/PIG/Summary.txt' USING PigStorage (',');
