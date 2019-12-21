-- job_skill 職缺所需技能
-- job_tool 職缺所需工具


CREATE TABLE `job_skill` (
  `source` varchar(4)  NOT NULL,
  `job_link` varchar(250)  NOT NULL,
  `category_top` varchar(20) DEFAULT NULL,
  `category_mid` varchar(20) DEFAULT NULL,
  `category_small` varchar(100) DEFAULT NULL,
  `skill` varchar(100)  NOT NULL
);

CREATE TABLE `job_tool` (
  `source` varchar(4)  NOT NULL,
  `job_link` varchar(250)  NOT NULL,
  `category_top` varchar(20) DEFAULT NULL,
  `category_mid` varchar(20) DEFAULT NULL,
  `category_small` varchar(100) DEFAULT NULL,
  `tool` varchar(100)  NOT NULL
);

------------ Done ------------
-- skill 137870 筆； tool 325407 筆