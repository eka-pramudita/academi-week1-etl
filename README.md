# ETL Pipeline Monitoring using Luigi

## What is ETL?
ETL is a process that extracts the data from different source systems, then 
transforms the data (like applying calculations, concatenations, etc.) 
and finally loads the data into the Data Warehouse system. ETL is a
fundamental process in Data Analytics since messy, noisy data would not
lead you anywhere even with the best data analytics method. In ETL process
we make sure that the data is ready to use to support business decision making.

<div align="center">
<img src="https://www.guru99.com/images/1/022218_0848_ETLExtractT1.png" >
</div>

## Luigi
Luigi is a Python package that helps you build complex pipelines of 
batch jobs. It handles dependency resolution, workflow management, 
visualization, handling failures, command line integration, and much more.
It was first written by Spotify and became open source in 2012. The interesting
part of Luigi is it has a Graphical User Interface (GUI) showing status
of the tasks and graphical representation of your pipeline.

<div align="center">
<img src="https://raw.githubusercontent.com/spotify/luigi/master/doc/luigi.png" >
</div>

### Data Pipeline Structure
The structure of a pipeline in Luigi, like one of many pipeline systems, 
resembles that of a graph. It contains nodes, where information is 
processed and edges connecting the nodes, transferring the information 
to the next node. The concept of Luigi is you have certain tasks and these tasks in turn may 
have dependencies on other tasks. Below is the example of ETL pipeline on Luigi:

<div align="center">
<img src="https://drive.google.com/uc?export=view&id=1a5TQv4FY0QKS8D0pCYBGgsGrbXF_6ZXk">
</div><br />

From above graph we can see that Drop Duplicates task is dependent with
Extract Completed task, meaning the Extract Completed task needs to be done
before Drop Duplicates task run. Drop Duplicates task also become a dependency
of 2 Merge and Load tasks above it.

## Extract Transform
In this project, extract and transform are put in a task. The task marked as completed 
when the extracted and transformed data stored into `src/extracted` folder. The data 
stored here then will be used in Load process. Below is the example of extract and 
transform task:
```
class Disaster(luigi.Task):

    completed = False

    def requires(self):

        return []

    def complete(self):

        return self.completed

    def run(self):

        disaster = pd.read_csv('./data/disaster_data.csv').to_csv('./src/extracted/disaster.csv',
                                                                  encoding='utf-8', index=False, header=True)

        self.completed = True
```

* `require()` represents dependency of the task
* `run()` represents process happened in a task
* `complete()` represents completion status of a task

Extract and Transform tasks are created according to the number of data sources. 
A task named ExtractCompleted with dependencies of all Extract and Transform tasks is
created to wrap the process. This task also becomes a dependency for every Load tasks created
after this.

### Data Processing Section
This section will discuss about how the data is being processed in this project.
Please find the data in folder `data/`.

#### Chinook Database
This database consists of 14 tables. Output resulted is 6 tables, coming from
joining several tables. Tables are as follows:

* Tracks - Contains complete information of tracks. Built from joining
tracks table with albums, artists, genres and media_types tables.
* Playlist_Track - Contains information of playlist where the tracks belong.
Built from joining playlist_track table with tracks and playlists tables.
* Invoice_Items - Contains information of tracks transacted. Built from joining
invoice_items table with tracks table.
* The rest are left as they are, those are Customers, Employees and Invoices
tables.

#### Database Sqlite
This database consists of 6 tables, resulting only 1 table. The result is
coming from joining reviews table with content, genres and labels tables.

#### Disaster
This data formed in .csv format, containing text about comments on disaster
happened. No transform needed for this data.

#### User
Stored in 2 sheets excel file with similar column names, 
the first process is to define table in every sheet. After adjusting column
names, both tables are being concatenated. Next step is remove duplicate of
the concatenated data, resulting 50 users information.

#### Reviews
Data source consists of 5 files, 2 files named q1_reviews stored in `.csv` and
`.xlsx` format. The first step is to validate data from both q1_reviews by
concatenating and removing duplicates. After we have cleaned q1_reviews,
we then concatenate it with q2-q4 reviews data, and remove duplicate once again.
The result shows that all observations from q1-q4 are similar.

#### Tweets
A json formatted data containing 30 fields. The first process is to read
json file by using pandas command `read_json`. Next step is to drop column
with no observations, resulting 29 fields in the final table.

## Load
We already have our extracted and transformed data in `src/extracted` folder.
First step of Load process is setting connection to the database that will
act as a data warehouse. After setting up a connection then store the data
using pandas command `to_sql`. Below is the example of load task:

```
class TweetLoader(luigi.Task):

    completed = False

    def requires(self):
        return [ExtractCompleted()]

    def complete(self):
        return self.completed

    def run(self):

        conn = sqlite3.connect('academi-etl-luigi.sqlite')

        pd.read_csv('./src/extracted/tweet.csv').to_sql('tweet', conn, if_exists="replace", index=False)

        conn.close()

        self.completed = True
```

Load tasks for all data are dependent with ExtractCompleted. A task named LoaderCompleted 
is created with dependencies of all Load tasks. The LoaderCompleted task then will be running
in the main program.

## Run Luigi
Please be sure that your cmd already inside your virtual environment when
running. If not, this [article](https://realpython.com/python-virtual-environments-a-primer/) may help.
First step is installing requirements needed by running this command:

```
pip install -r requirements.txt
```

Then set up Luigi GUI by running this command in a cmd:
```
luigid
```

Then open `localhost:8020` in your browser and you will find this empty GUI:
<div align="center">
<img src="https://drive.google.com/uc?export=view&id=1yhZAF_BEBmMt8WZKDQvB06QPQqgNPqr2">
</div><br />

After that, open another cmd then run the program by running this command:
```
python main.py
```

You will find your dashboard running through the GUI:
<div align="center">
<img src="https://drive.google.com/uc?export=view&id=1uUxPt2-iXs4fL4MCIJfONOxw5RF_p_uh">
<small> Pending task appeared </small>
</div><br />

When all tasks are done, the dashboard will show no pending tasks.
The pipeline visualization will be looked like this:
<div align="center">
<img src="https://drive.google.com/uc?export=view&id=1vj_JPCn8Cq4DICrKyQTcAfd8vihjMjuJ">
<small> All tasks completed </small>
</div><br />

## Summary
Since ETL process is fundamental in Data Analytics process, this process needs
to be carefully monitored. Using Luigi, the ETL pipeline monitoring is easy
since it has powerful GUI for task status and pipeline.

<div align="center">
<img src="https://drive.google.com/uc?export=view&id=1-7hKKD56qWRfr3aV0CO9eCyaROeEKBxl">
<small> Another pipeline graph option </small>
</div><br />