# CA675-assignment-1

1.  Acquire the top 200,000 posts by viewcount (via https://data.stackexchange.com/)  

2.  Using Pig or MapReduce, extract, transform and load the data as applicable 

3.  Using Hive and/or MapReduce, get:
      I.    The top 10 posts by score
      II.   The top 10 users by post score
      III.  The number of distinct users, who used the word “Hadoop” in one of
            their posts

4.  Using Mapreduce calculate the per-user TF-IDF (just submit the top 10 terms for each of the top 10 users from Query 3.

# Part 1 - Data Acquisition

In order to perform the tasks contained in this assignment, we first need to obtain our data from the Stack Exchange Data Explorer site (link below). The site allows you to access all posts made on the website and export your output as a csv. There's a max file size limit of 50,000 posts therefore we will have to perform 4 queries in order to get the top 200,000 posts on the website.

Link to Data - https://data.stackexchange.com/

#### Query 1

Extract the top 50,000 posts. I'm doing this by using the TOP function. In order to create a rank of the posts, I'm using the ORDER BY function by ordering ViewCount in descending order and id by ascending order. This creates an ordered output of the top 50,000 posts on the Stack Exchange website. I've also added a WHERE clause to return all posts with more than 2,000 views in order to improve the efficiency of my query.

```
SELECT TOP 50000 * 
FROM posts where posts.ViewCount > 20000 
ORDER BY posts.ViewCount DESC, posts.id ASC;
```

#### Query 2

In the next stage, I'm using the TOP function again to collect my next 50,000 posts from Stack Exchange. I'm using the same ranking functions as previously. In order to remove the the top 1-50,000 posts, I'm using a NOT IN sub-query.

This enables me to collect the top 50,001 - 100,000 posts on the website.

```
SELECT TOP 50000 *
FROM posts
where posts.ViewCount > 20000
and posts.id NOT IN
(
SELECT TOP 50000 posts.id 
FROM posts 
ORDER BY posts.ViewCount DESC, posts.id ASC
) 
ORDER BY posts.ViewCount DESC, posts.id ASC;
```

#### Query 3

In the next stage, I'm using the TOP function again to collect my next 50,000 posts from Stack Exchange. I'm using the same ranking functions as previously. In order to remove the the top 1-100,000 posts, I'm using a NOT IN sub-query and adjusting it to remove the top 100,000 posts through my ordered ranking function.

This enables me to collect the top 100,001 - 150,000 posts on the website.

```
SELECT TOP 50000 *
FROM posts
where posts.ViewCount > 20000
and posts.id NOT IN
(
  SELECT TOP 100000 posts.id
  FROM posts
  ORDER BY posts.ViewCount DESC
)
ORDER BY posts.ViewCount DESC;
```

#### Query 4

In the next stage, I'm using the TOP function again to collect my final 50,000 posts from Stack Exchange. I'm using the same ranking functions as previously. In order to remove the the top 1-150,000 posts, I'm using a NOT IN sub-query and adjusting it to remove the top 150,000 posts through my ordered ranking function.

This enables me to collect the top 150,001 - 200,000 posts on the website.

```
SELECT TOP 50000 *
FROM posts
where posts.ViewCount > 20000
and posts.id NOT IN
(
  SELECT TOP 150000 posts.id
  FROM posts
  ORDER BY posts.ViewCount DESC
)
ORDER BY posts.ViewCount DESC;
```

These 4 queries give me the top 200,000 posts on Stack Exchange which I will later use in the Hadoop system. 

# Part 2 - Pig ETL Process

Now that we have our 200,000 posts ready for loading into our cluster, we need to create our working directory where we will load our 4 files. We do so by checking our current directories and creating the new working directory

```
-- VIEW CURRENT DIRECTORIES
hadoop fs –ls /

--CREATE NEW WORKING DIRECTORY CALLED 'top200'
hadoop fs -mkdir /top200
```
Next we upload our 4 csv files into Hadoop. The first stage is using the 'Upload Files' function. Once the files are in the cluster, we will place them into Hadoop in our newly create working directory. 

```
hadoop fs -put 0to50.csv /top200
hadoop fs -put 50to100.csv /top200
hadoop fs -put 100to150.csv /top200
hadoop fs -put 150to200.csv /top200
```

We're now ready to fire up Pig. This is done as below.

The first step will be to load our 4 files into Pig to be transformed and loaded into Hive for querying later. I'm using the CSVExcelStorage function from the Piggybank class in order to do this. 

* file is comma delimited (',')
* I'm allowing multiline to account for line breaks which exist within the body field in    my dataset
* I'm skipping my input header as i will be loading my dataset into Hive later for querying so they're not needed at this stage.
* At this stage, i'm declaring the column name and datatype of each column

```
top200_merged = LOAD '/top200/*.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','YES_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER') AS(Id:int, PostTypeId:int, AcceptedAnswerId:int, ParentId:int, CreationDate:datetime, DeletionDate:datetime, Score:int, ViewCount:int, Body:chararray, OwnerUserId:int, OwnerDisplayName:chararray, LastEditorUserId:int, LastEditorDisplayName:chararray, LastEditDate:datetime, LastActivityDate:datetime, Title:chararray, Tags:chararray, AnswerCount:int, CommentCount:int, FavoriteCount:int, ClosedDate:datetime, CommunityOwnedDate:datetime);
```

Next step, I'm running a DISTINCT check on my dataset. This will eliminate the potential for any duplication across my dataset, should I have made an error within the data acquisition stage.

```
top200_merged_distinct = DISTINCT top200_merged;
```

Next, I begin my transformation process in order to facilitate part 3. Firstly I strip out any columns that are not needed to complete the assignment. This will improve the performance of my queries and reduce costs within my cluster. I only need 4 columns at this stage; Id, Score, OwnerUserId & Body. 

```
top200_uncleaned_filtered = FOREACH top200_merged_distinct GENERATE Id,Score,OwnerUserId,Body;
```

Now I want to begin the cleansing of my Body column for usage within the TF-IDF part of the assignment. Using the FOREACH function, i'm returning each record of my new shortened dataset. With the Body column, I'm running a replace function to find any HTML tags (string that is surrounded by <>) and replacing it with a blank (''). This will remove potential impact on word frequency for when I get to the TF-IDF section of the assignment. 

```
top200_cleaned_filtered_1 = FOREACH top200_uncleaned_filtered GENERATE Id,Score,OwnerUserId,REPLACE(Body, '<.*?.>', '') AS Body;
```

In the second stage of my cleansing process, I want to remove any line breaks that exist within the Body column. Using the same methodology as above, I use the FOREACH and REPLACE functions together to find line breaks ('\n') and replace them with a single space (' '). This negates issues with my Hive table creation later. Without running this, records that have line breaks cause multiple rows in my table. These would have corresponding NULL values for my 3 other columns as well as return a table with >200,000 as a result.

```
top200_cleaned_filtered_2 = FOREACH top200_cleaned_filtered_1 GENERATE Id,Score,OwnerUserId,REPLACE(Body, '\n', ' ') AS Body;
```

Finally with our new cleansed and transformed dataset, we store our output into my newly created working directory (/cleaned_data). Again I'm using the CSVExcelStorage function available from the Piggybank class. 

* As a precautionary measure, I'm switching my delimiter to a pipe ('|'). I'm unsure currently of the impact of using a comma delimiter later, particularly as my Body column may still contain commas within the string. 
* I'm switching to  NO_MULTILINE to account for my removal of line breaks from my dataset
* Again I'm skipping my output header as i will be loading my dataset into Hive later for querying so headers are still not needed at this stage.

```
STORE top200_cleaned_filtered_2 INTO '/cleaned_data' USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','NO_MULTILINE','NOCHANGE','SKIP_OUTPUT_HEADER');
```

At this stage I'm ready to move to Hive to begin my data querying.

