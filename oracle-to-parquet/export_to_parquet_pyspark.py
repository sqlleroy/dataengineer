from pyspark import SQLContext, SparkContext, SparkConf
import json
import gc

try:
    # ------------------------ #
    # Creating a Spark Context #
    # ------------------------ #
    
        # -------------------------------------------- #
        # To test later: spark.debug.maxToStringFields #
        # -------------------------------------------- #    
    config = SparkConf().setAll(
        [
            ("spark.sql.execution.arrow.enabled","true",),  # Enable Arrow-based columnar data transfers
            ("spark.executor.cores", "4"),
            ("spark.cores.max", "4"),
            ("spark.driver.memory", "9g"),
            ("spark.sql.parquet.compression.codec", "gzip"),
        ]
    )
    sc = SparkContext(conf=config)
    sqlcontext = SQLContext(sc)

    # ------------------ #
    # Oracle definitions #
    # ------------------ #
    # Open the connection with Oracle Database
    json_file = open("./config/config.json")
    ora_config = json.load(json_file)

    dsn = ora_config["dsn"]
    conn = "jdbc:oracle:thin:@//{iDsn}".format(iDsn=dsn)
    user = ora_config["username"]
    # Note: Enhance it to use secreats instead.
    pwd = ora_config["password"]

    # ------------------------------------------------- #
    # Reading Json File with List of schemas and Tables #
    # ------------------------------------------------- #
    json_file = open(".\\config\\tables.json")
    parameters = json.load(json_file)

    for (key, schemas) in parameters.items():
        for schema in schemas:
            table_owner = schema["owner"]
            for table in schema["tables"]:
                table_name = table

                # --------------------------------------- #
                # Dictionary to use on all string formats #
                # --------------------------------------- #
                Dic = {"Table": table_name, "Owner": table_owner, "Partition": ""}

                # ----------------------- #
                # Query: Metadata Catalog -------------------------- #
                # Description: - Looking for the table and its owner #
                # -------------------------------------------------- #  
                sql0 = "SELECT TABLE_NAME FROM DBA_TABLES WHERE TABLE_NAME='{iDic[Table]}' AND OWNER='{iDic[Owner]}'".format(iDic=Dic)
                Table = (
                    sqlcontext.read.format("jdbc")
                    .options(
                        url=conn,
                        driver="oracle.jdbc.OracleDriver",
                        query=sql0,
                        user=user,
                        password=pwd,
                    )
                    .load()
                )

                # --------------- #
                # If table exists #
                # --------------- #
                if Table:
                    print("Exporting {iDic[Owner]}.{iDic[Table]}".format(iDic=Dic))
                    print("--> Collecting Oracle partition metadata to export the file")

                    # ----------------------- #
                    # Query: Metadata Catalog --------------------------------------------------------------#
                    # Description: Collect the partitions values to use later on while filtering the table. #
                    # ------------------------------------------------------------------------------------- #
                    sql1 = "SELECT PARTITION_NAME FROM DBA_TAB_PARTITIONS WHERE TABLE_NAME='{iDic[Table]}' AND TABLE_OWNER='{iDic[Owner]}' ORDER BY PARTITION_POSITION ASC".format(iDic=Dic)
                    Partitions = (
                        sqlcontext.read.format("jdbc")
                        .options(
                            url=conn,
                            driver="oracle.jdbc.OracleDriver",
                            query=sql1,
                            user=user,
                            password=pwd,
                        )
                        .load()
                    )

                    # ------------------------------------------------------------------ # 
                    # Note: Converting it to Pandas to be able to loop using iterrows(). #
                    # Didn't find a way of doing it directly from Spark DF               #
                    # ------------------------------------------------------------------ # 
                    Partitions_dfp = Partitions.toPandas()

                    # ------------------------------------------- #
                    # Loop based on partitions to export the file #
                    # ------------------------------------------- #
                    if Partitions_dfp.empty == False:
                        for index, partition_name in Partitions_dfp.iterrows():

                            partition = partition_name["PARTITION_NAME"]

                            # ----------------------------------------------------- #
                            # Updating the Dictionary [Dic] with the partition name #
                            # ----------------------------------------------------- #
                            Dic.update(Partition=partition)

                            # -------------------- #                            
                            # Query: Table DataSet ----------------------------- #
                            # Description: Select data from the target partition #
                            # -------------------------------------------------- #
                            sql2 = "SELECT /*+ PARALLEL(20) */ * FROM {iDic[Owner]}.{iDic[Table]} partition ({iDic[Partition]})".format(iDic=Dic)

                            print("--> Querying Partition {iDic[Partition]}".format(iDic=Dic))

                            # ---------------------------- #
                            # Runing the query using Spark #
                            # ---------------------------- #
                            dfs = (
                                sqlcontext.read.format("jdbc")
                                .options(
                                    url=conn,
                                    driver="oracle.jdbc.OracleDriver",
                                    query=sql2,
                                    user=user,
                                    password=pwd,
                                    fetchsize=200000,
                                )
                                .load()
                                .persist() # Reference: https://sparkbyexamples.com/spark/spark-dataframe-cache-and-persist-explained/
                            )

                            print("--> Exporting Partition {iDic[Partition]}".format(iDic=Dic))

                            # ------------------------------------- #
                            # Defining the file name to be exported #
                            # ------------------------------------- #
                            path = ".\\files\\{iDic[Owner]}_{iDic[Table]}_{iDic[Partition]}".format(iDic=Dic)

                            # -------------------------------------------------- #
                            # Exporting the parquet file using Spark Repartition ----------------------------------------------------- #
                            # Reference: https://kontext.tech/column/spark/296/data-partitioning-in-spark-pyspark-in-depth-walkthrough #
                            # -------------------------------------------------------------------------------------------------------- #
                            # df = dfs.repartition(4)
                            dfs.repartition("ColumnName").write.partitionBy("ColumnName").mode(SaveMode.Append).parquet(path)                                                        

                            # --------------- #
                            # Memory Cleaning #
                            # --------------- #
                            dfs.unpersist()
                            del dfs
                            del df
                            gc.collect()

                    else: # No Partitions

                        print("--> There is no Partition for the table {iDic[Table]}. A single parquet will be exported for this table".format(iDic=Dic))

                        # -------------------- #                            
                        # Query: Table DataSet ----------------------------- #
                        # Description: Select data from the target partition #
                        # -------------------------------------------------- #
                        sql2 = "SELECT /*+ PARALLEL(20) */ * FROM {iDic[Owner]}.{iDic[Table]}".format(iDic=Dic)

                        print("--> Querying Table {iDic[Table]}".format(iDic=Dic))

                        # ---------------------------- #
                        # Runing the query using Spark #
                        # ---------------------------- #
                        dfs = (
                            sqlcontext.read.format("jdbc")
                            .options(
                                url=conn,
                                driver="oracle.jdbc.OracleDriver",
                                query=sql2,
                                user=user,
                                password=pwd,
                                fetchsize=200000,
                            )
                            .load()
                        )

                        print("--> Exporting table {iDic[Table]}".format(iDic=Dic))

                        # ------------------------------------- #
                        # Defining the file name to be exported #
                        # ------------------------------------- #
                        filename = ".\\files\\{iDic[Owner]}_{iDic[Table]}.parquet".format(iDic=Dic)

                        # -------------------------------------------------- #
                        # Exporting the parquet file using Spark Repartition ----------------------------------------------------- #
                        # Reference: https://kontext.tech/column/spark/296/data-partitioning-in-spark-pyspark-in-depth-walkthrough #
                        # -------------------------------------------------------------------------------------------------------- #
                        df = dfs.repartition(4).write.option("compression", "snappy").mode("overwrite").save(filename)

                    # --------------- #
                    # Memory Cleaning #
                    # --------------- #
                    dfs.unpersist()
                    del dfs
                    del df
                    gc.collect()
    
                else:
                    print("Table {iDic[Owner]}.{iDic[Table]} does not exist or you don't have access to it.".format(iDic=Dic))

            # --------------- #
            # Memory Cleaning #
            # --------------- #
            gc.collect()

except Exception as error:
    print(error)
finally:
    if json_file:
        json_file.close()
