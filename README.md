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
