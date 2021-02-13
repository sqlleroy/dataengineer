import ora_connection as ora
import pandas as pd
import json
import gc

try:
    # Open the connection with Oracle Database
    oracle = ora.open_connection()

    # Parameters - note: enhancing this part to read a json file with a list of tables and owners
    json_file = open(".\\config\\tables.json")
    parameters = json.load(json_file)

    for (key, schemas) in parameters.items():
        for schema in schemas:
            table_owner = schema["owner"]
            for table in schema["tables"]:
                table_name = table

                # Dictionary to use on all string formats
                Dic = {"Table": table_name, "Owner": table_owner, "Partition": ""}

                sql0 = "SELECT TABLE_NAME FROM DBA_TABLES WHERE TABLE_NAME='{iDic[Table]}' AND OWNER='{iDic[Owner]}'".format(iDic=Dic)
                Table = pd.read_sql_query(sql0, oracle)

                # If table exists
                if Table.empty == False:
                    print("Exporting {iDic[Owner]}.{iDic[Table]}".format(iDic=Dic))
                    print("--> Collecting Oracle partition metadata to export the file")

                    # Query to collect the partitions values to use later on filtering the table.
                    sql1 = "SELECT PARTITION_NAME FROM DBA_TAB_PARTITIONS WHERE TABLE_NAME='{iDic[Table]}' AND TABLE_OWNER='{iDic[Owner]}' ORDER BY PARTITION_POSITION DESC".format(iDic=Dic)
                    Partitions = pd.read_sql_query(sql1, oracle)

                    # chunk_list = []
                    df_result = pd.DataFrame()
                    # Loop based on partitions to export the file
                    if Partitions.empty == False:
                        for index, partition_name in Partitions.iterrows():
                            partition = partition_name["PARTITION_NAME"]

                            Dic.update(Partition=partition)

                            # Query to select data from the target partition
                            sql2 = "SELECT /*+ PARALLEL(20) */ * FROM {iDic[Owner]}.{iDic[Table]} partition ({iDic[Partition]})".format(iDic=Dic)

                            print("--> Querying Partition {iDic[Partition]}".format(iDic=Dic))

                            # Runing the query using pandas
                            for chunk in pd.read_sql_query(sql=sql2, con=oracle, chunksize=200000):
                                # chunk_list.append(chunk)
                                df_result = df_result.append(chunk, ignore_index=True)

                            # df_result = pd.concat(chunk_list)

                            print("--> Exporting Partition {iDic[Partition]}".format(iDic=Dic))

                            # Defining the file name to be exported
                            filename = ".\\files\\{iDic[Owner]}_{iDic[Table]}_{iDic[Partition]}.parquet".format(iDic=Dic)

                            # Exporting the parquet file
                            df_result.to_parquet(filename, index=False)
                            # df_result.to_parquet(filename, engine='fastparquet',
                            #                      compression='gzip')

                            del df_result
                            gc.collect()
                            df_result = pd.DataFrame()

                    else:
                        print("--> There is no Partition for the table {iDic[Table]}. A single parquet will be exported for this table".format(iDic=Dic))

                        # Query to select data from the target table from previous partition to the current partition
                        sql2 = "SELECT /*+ PARALLEL(20) */ * FROM {iDic[Owner]}.{iDic[Table]}".format(iDic=Dic)

                        print("--> Querying Table {iDic[Table]}".format(iDic=Dic))

                        # Runing the query using pandas
                        for chunk in pd.read_sql_query(
                            sql=sql2, con=oracle, chunksize=200000
                        ):
                            # chunk_list.append(chunk)
                            df_result = df_result.append(chunk, ignore_index=True)

                        # df_result = pd.concat(chunk_list)

                        print("--> Exporting table {iDic[Table]}".format(iDic=Dic))

                        # Defining the file name to be exported
                        filename = ".\\files\\{iDic[Owner]}_{iDic[Table]}.parquet".format(iDic=Dic)

                        # Exporting the parquet file
                        df_result.to_parquet(filename, index=False)

                    del [[Partitions, df_result]]
                    gc.collect()
                    Partitions = pd.DataFrame()
                    df_result = pd.DataFrame()

                else:
                    print("Table {iDic[Owner]}.{iDic[Table]} does not exist or you don't have access to it.".format(iDic=Dic))

            del [[Table, Partitions, df_result]]
            gc.collect()
            Table = pd.DataFrame()
            Partitions = pd.DataFrame()
            df_result = pd.DataFrame()

    if json_file:
        json_file.close()

    if oracle:
        oracle.close()

except oracle.Error as error:
    print(error)
