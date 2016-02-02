register $baseDir/libs/ginko.jar;
define SIMSCORE io.enjapan.il.ginko.CompanySimilarity;

lbc = LOAD '$baseDir/data/test/lbc_2.csv' USING PigStorage(',') as(office_id:long,
    company_name:chararray, company_name_kana:chararray,representative:chararray,
    representative_kana:chararray, company_tel:chararray,company_vitality:chararray,ind1:int,
    ind2:int,ind3:int, company_zip:chararray, prefecture:chararray,county:chararray,city:chararray,
    ward:chararray, address:chararray, is_listed:int,company_age:int,capital:int,emp_count_est:int,
    sales_est:int, income_est:int, office_count:int,name_pref:chararray,name_pref_tel:chararray,
    locality:chararray, id2:long);
lbc = FOREACH lbc GENERATE
    office_id,company_name,representative,company_tel,company_zip,
    prefecture,county,ward,city,address,is_listed,company_age,capital,emp_count_est, id2;
SPLIT lbc INTO X if id2 > 5L, Y IF id2 <=5L;
Y2 = FOREACH Y GENERATE *, id2+5L as id3:long;
joined = JOIN X BY id2, Y2 BY id3;
j2 = FOREACH joined GENERATE
    X::office_id as x_id:long, Y2::office_id as y_id:long,
    TOTUPLE(X::company_name,X::representative,X::company_tel,X::prefecture,
    X::county,X::ward,X::city,X::address,X::company_zip,'www.url.com',X::is_listed,
    X::company_age,X::capital) as c1, TOTUPLE(Y2::company_name,Y2::representative,
    Y2::company_tel,Y2::prefecture, Y2::county,Y2::ward,Y2::city,Y2::address,Y2::company_zip,
    'www.url.com',Y2::is_listed, Y2::company_age,Y2::capital) as c2;
j3 = LIMIT j2  2;
scored = FOREACH j3 GENERATE x_id, y_id, SIMSCORE(c1,c2) as simscore:double;
dump scored;
