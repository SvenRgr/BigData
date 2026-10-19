CREATE EXTERNAL TABLE IF NOT EXISTS name_basics (
	nconst STRING,
	primaryName STRING,
	birthYear DECIMAL(4,0),
	deathYear DECIMAL(4,0),
	primaryProfession STRING,
	knownForTitles STRING
) COMMENT 'IMDb Names' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE LOCATION '/user/hadoop/imdb/name_basics'
TBLPROPERTIES ('skip.header.line.count'='1');
