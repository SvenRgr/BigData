# Exercise 2: Solutions for Pages 22 and 23

These commands and HiveQL queries are solutions only; they have not been
executed. Actual results depend on the downloaded IMDb dataset.
The queries use the column names from the existing table definitions.

## 1. Complete the preceding hands-on tasks

Set up Hive, start HDFS and YARN, and import `title.basics.tsv` and
`title.ratings.tsv` as shown on pages 5-16. Create the external tables
`title_basics` and `title_ratings`, then test them:

```sql
SHOW DATABASES;
SELECT * FROM title_basics LIMIT 3;
SELECT * FROM title_ratings LIMIT 3;
```

The example join from pages 17-18 is:

```sql
SELECT basics.*, ratings.*
FROM title_basics basics
JOIN title_ratings ratings ON basics.tconst = ratings.tconst
WHERE basics.original_title = 'The Dark Knight'
  AND basics.title_type = 'movie';
```

Note: `average_rating` should use `DECIMAL(3,1)`, rather than the slides'
`DECIMAL(2,1)`, so that a rating of `10.0` can be represented.

## 2-3. Download names and transfer them to HDFS

Run these shell commands as the `hadoop` user inside the container,
not at the Hive prompt:

```bash
cd /home/hadoop
wget https://datasets.imdbws.com/name.basics.tsv.gz
gunzip name.basics.tsv.gz
hadoop fs -mkdir -p /user/hadoop/imdb/name_basics
hadoop fs -put name.basics.tsv /user/hadoop/imdb/name_basics/name.basics.tsv
```

## 4. Create the external names table

Run this at the Hive prompt. The column order matches the TSV file;
IMDb's missing-value marker `\N` is read as `NULL` by the default SerDe.

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS name_basics (
    nconst STRING,
    primaryName STRING,
    birthYear DECIMAL(4,0),
    deathYear DECIMAL(4,0),
    primaryProfession STRING,
    knownForTitles STRING
)
COMMENT 'IMDb Names'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/hadoop/imdb/name_basics'
TBLPROPERTIES ('skip.header.line.count'='1');
```

## 5a. Number of movies and TV series

```sql
SELECT title_type, COUNT(*) AS title_count
FROM title_basics
WHERE title_type IN ('movie', 'tvSeries')
GROUP BY title_type
ORDER BY title_type;
```

This returns separate counts for movies and TV series. TV episodes and
TV miniseries are not included. The previously recorded total of 12,858,131
counted all title types, so it does not answer this question.

## 5b. Youngest person in the dataset

```sql
SELECT names.nconst,
       names.primaryName,
       names.birthYear,
       names.primaryProfession
FROM name_basics names
JOIN (
    SELECT MAX(birthYear) AS youngest_birth_year
    FROM name_basics
    WHERE birthYear IS NOT NULL
) youngest ON names.birthYear = youngest.youngest_birth_year
ORDER BY names.primaryName, names.nconst;
```

The latest known birth year identifies the youngest people at year-level
precision. This returns all ties and excludes unknown birth years. The
question's "actor/writer/..." is interpreted as any person in `name_basics`.
The previously recorded "Sean Jones, 1979" has not been verified and is
not presented as a confirmed answer.

## 5c. Highly rated movies released in 2010 or later

```sql
SELECT basics.tconst,
       basics.original_title,
       basics.start_year,
       ratings.average_rating,
       ratings.num_votes
FROM title_basics basics
JOIN title_ratings ratings ON basics.tconst = ratings.tconst
WHERE basics.title_type = 'movie'
  AND basics.start_year >= 2010
  AND ratings.average_rating >= 8.1
  AND ratings.num_votes > 100000
ORDER BY ratings.average_rating DESC,
         ratings.num_votes DESC,
         basics.tconst;
```

Both the year and rating thresholds are inclusive; the vote threshold is
strictly greater than 100,000. Missing ratings or years do not qualify.

## 5d. Number of movies from 5c

```sql
SELECT COUNT(*) AS qualifying_movie_count
FROM title_basics basics
JOIN title_ratings ratings ON basics.tconst = ratings.tconst
WHERE basics.title_type = 'movie'
  AND basics.start_year >= 2010
  AND ratings.average_rating >= 8.1
  AND ratings.num_votes > 100000;
```

This uses exactly the same filters as 5c without creating an intermediate
table. Previously recorded result: **74** (not verified here).

## 5e. Best cinema years by number of highly rated movies

```sql
SELECT basics.start_year,
       COUNT(*) AS movie_count
FROM title_basics basics
JOIN title_ratings ratings ON basics.tconst = ratings.tconst
WHERE basics.title_type = 'movie'
  AND basics.start_year IS NOT NULL
  AND ratings.average_rating > 8.0
  AND ratings.num_votes > 100000
GROUP BY basics.start_year
ORDER BY movie_count DESC, basics.start_year ASC;
```

Unlike 5c, this query considers all known release years, not just 2010
onwards. Only years with qualifying movies appear. Ratings must be
strictly greater than 8 and votes strictly greater than 100,000; ties
in movie count are ordered by year.