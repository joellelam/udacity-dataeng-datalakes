# Introduction

The goal of this project is to load Sparkify data from S3 into an analytics table using Spark. Then, the contents will be loaded back into s3 as parquet.

# Python Assets

Files: 
* etl.py > This is the primary application used to load the data from s3 and generate the parquet files. 
* dl.cfg > Configuration file to hold access rights.

`etl.py`

Examples: 

`etl.py -h`

`etl.py -i data`

`etl.py -i data -i out_data`
    
# JSON Input Data Schema

Below is a description of the data schema per json files provided for both song files and log fies.

![JSON Schema](/images/staging_schema.png)

Sample dataset from song file.

    {
        "num_songs": 1, 
        "artist_id": "ARJIE2Y1187B994AB7", 
        "artist_latitude": null, 
        "artist_longitude": null, 
        "artist_location": "", 
        "artist_name": "Line Renaud", 
        "song_id": "SOUPIRU12A6D4FA1E1", 
        "title": "Der Kleine Dompfaff", 
        "duration": 152.92036, 
        "year": 0
    }
    
Sample dataset from log file.

    {
        "artist":"Stephen Lynch",
        "auth":"Logged In",
        "firstName":"Jayden",
        "gender":"M",
        "itemInSession":0,
        "lastName":"Bell",
        "length":182.85669,
        "level":"free",
        "location":"Dallas-Fort Worth-Arlington, TX",
        "method":"PUT",
        "page":"NextSong",
        "registration":1540991795796.0,
        "sessionId":829,
        "song":"Jim Henson's Dead",
        "status":200,
        "ts":1543537327796,
        "userAgent":"Mozilla\/5.0 (compatible; MSIE 10.0; Windows NT 6.2; WOW64; Trident\/6.0)",
        "userId":"91"
    }


# Final Database Schema
The target table schema is setup as a star schema in which songplays is the fact table and 
other surrounding tables are the dimensions.  This table is denormalized because it has some 
redundant data in the songplays db, for example location.  However, redundant data is 
inserted redundantly to optimize for analytical queries. 

In the fact table, logs are inserted such that you can know what a user has listened by both artists and song id 
when you merge the dimension query.  However, you can directly query things like level.

![Star Schema](/images/star_schema.png)

# Output files
The output files represent the start schema database in parquet form.  

Samples of how to read from parquet files.
```
songs_df = spark.read.parquet("s3n://udacityproject4/songs.parquet")
songs_df.createOrReplaceTempView("songs")
val namesDF = spark.sql("SELECT * FROM songs")

```

