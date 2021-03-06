# Udacity Data Engineer Nanodegree

![certificate](certificate.png)

## Completed Projects

1. Data Modeling: Model data with Postgresql and Apache Cassandra.
2. Cloud Data Warehouses: Use AWS Redshift to build a data pipeline of data warehouse.
3. Data Lake with Spark: Use Apache Spark to build a data pipeline of data lake.
4. Data Pipeline wiht Airflow: Use Apache Airflow to build a scheduler of a data pipeline.
5. Capstone Project: Use data provided by Udacity to build an ETL data pipeline wiht the technologies learned in this Nanodegree.

## Memo

### MySQL docker

```
# create mysql container with docker-network: udacity-network and custom paths for conf.d and data
docker run --name udacity-mysql --network udacity-network -v absolute-path-to-conf.d:/etc/mysql/conf.d -v absoulte-path-to-mysql-data:/var/lib/mysql -e MYSQL_ROOT_PASSWORD=root-password -p 3306:3306 -d mysql:5.7

# connect to mysql running in docker container
docker run -it --rm --network udacity-network mysql:5.7 mysql -hudacity-mysql -uroot -proot-password
```

