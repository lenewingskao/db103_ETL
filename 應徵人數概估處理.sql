-- 將應徵人數(分類)多一欄概估數值
-- Type1 = 5, type2=20, type3=35


-- 將概估值新增到暫時表
create table temp (
job_link varchar(250),
candi_estimate smallint
);

insert into temp (
select job_link, 
case 
	when candidates='1' then 5 
	when candidates='2' then 20 
	when candidates='3' then 35 
end as candi_estimate 
from job
);

-- 將暫時表資料異動回job

ALTER TABLE job ADD COLUMN candi_estimate smallint;

UPDATE job a
INNER JOIN temp b ON a.job_link = b.job_link
SET a.candi_estimate = b.candi_estimate ;

-- out memory 的話 增加 where a.category_top ='管理財經' 職缺大類分六次run