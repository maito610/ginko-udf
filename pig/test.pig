lbc = LOAD '../data/lbc2.csv' USING PigStorage(',','-schema');
lbc2 = FOREACH lbc GENERATE office_id,company_name,representative,company_tel,company_zip,prefecture,county,ward,city,address,is_listed,company_age,capital,emp_count_est;
