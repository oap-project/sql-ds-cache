#include <stdlib.h>

#include <iostream>
#include <memory>

#include <arrow/api.h>
#include <arrow/filesystem/api.h>
#include <parquet/api/reader.h>
#include <gtest/gtest.h>
#include <vector>

#include "src/reader.h"
#include "src/utils/PredicateExpression.h"

TEST(predicateFilterTest, minMaxTest)
{
    arrow::fs::HdfsOptions options_;

    std::string hdfs_host = "clx06-AEP";
    int hdfs_port = 8020;

    options_.ConfigureEndPoint(hdfs_host, hdfs_port);

    auto result = arrow::fs::HadoopFileSystem::Make(options_);
    EXPECT_TRUE(result.ok()) << "HadoopFileSystem Make failed";

    std::shared_ptr<arrow::fs::FileSystem> fs_ =
        std::make_shared<arrow::fs::SubTreeFileSystem>("", *result);

    std::string file_name =
        "/user/hive/warehouse/tpcds_hdfs_parquet_10.db/store_sales/ss_sold_date_sk=2450817/"
        "part-00013-0828d1ab-ef1f-4b55-bf94-c071fb76c353.c000.snappy.parquet";

    auto file_result = fs_->OpenInputFile(file_name);
    EXPECT_TRUE(file_result.ok()) << "Open hdfs file failed";

    std::shared_ptr<arrow::io::RandomAccessFile> file = file_result.ValueOrDie();
    std::cout << "file size is " << file->GetSize().ValueOrDie() << std::endl;

    parquet::ReaderProperties properties;
    // std::shared_ptr<parquet::FileMetaData> metadata;
    std::unique_ptr<parquet::ParquetFileReader> parquetReader =
        parquet::ParquetFileReader::Open(file, properties, NULLPTR);

    std::shared_ptr<parquet::FileMetaData> fileMetaData = parquetReader->metadata();

    int numRows = fileMetaData->num_rows();
    int numCols = fileMetaData->num_columns();
    int numRowGroups = fileMetaData->num_row_groups();
    std::string schema = fileMetaData->schema()->ToString();

    std::cout<<"parquet file has "<<numRows<<" rows"<<std::endl
                                <<numCols<<" cols"<<std::endl
                                <<numRowGroups<<" row groups"<<std::endl;

    std::cout<<"schema: "<<schema<<std::endl;

    if(numRowGroups < 1)
    {
        std::cout<<"not enough row groups for test"<<std::endl;
        return;
    }

    int rowGroupIndex = 0;
    std::unique_ptr<parquet::RowGroupMetaData> urgMataData = fileMetaData->RowGroup(rowGroupIndex);
    std::shared_ptr<parquet::RowGroupMetaData> rgMataData = std::move(urgMataData);

    //std::vector<std::unique_ptr<parquet::ColumnChunkMetaData>> columnChunkMeta;
    //columnChunkMeta.resize(numCols);
    std::unique_ptr<parquet::ColumnChunkMetaData> columnChunkMeta;

    for(int i=0; i<numCols; i++)
    {
        columnChunkMeta = rgMataData->ColumnChunk(i);
        std::string column_name = fileMetaData->schema()->Column(i)->name();
        parquet::Type::type column_type = fileMetaData->schema()->Column(i)->physical_type();
        std::cout<<"current column["<<i<<"]: "<<column_name
                                    <<" type "<<column_type
                                    <<std::endl;
        
        std::shared_ptr<parquet::Statistics> statistic = columnChunkMeta->statistics();
        if(!statistic->HasMinMax())
        {
            std::cout<<"This column does not have valid min max value."<<std::endl;
            continue;
        }
        switch (column_type)
        {
        case parquet::Type::BOOLEAN:{
            auto boolStatistic = std::static_pointer_cast<parquet::BoolStatistics>(statistic);
            std::cout<<"min: "<<boolStatistic->min()<<" max: "<<boolStatistic->max()<<std::endl;
            break;}
        case parquet::Type::INT32:{
            auto int32Statistic = std::static_pointer_cast<parquet::Int32Statistics>(statistic);
            std::cout<<"min: "<<int32Statistic->min()<<" max: "<<int32Statistic->max()<<std::endl;
            break;}
        case parquet::Type::INT64:{
            auto int64Statistic = std::static_pointer_cast<parquet::Int64Statistics>(statistic);
            std::cout<<"min: "<<int64Statistic->min()<<" max: "<<int64Statistic->max()<<std::endl;
            break;}
        case parquet::Type::FLOAT:{
            auto floatStatistic = std::static_pointer_cast<parquet::FloatStatistics>(statistic);
            std::cout<<"min: "<<floatStatistic->min()<<" max: "<<floatStatistic->max()<<std::endl;
            break;}
        
        default:
            break;
        }
        
    }

}

TEST(predicateFilterTest, predicateTest) {

    std::string hdfs_host = "clx06-AEP";
    int hdfs_port = 8020;
    
    std::string file_name =
        "/user/hive/warehouse/tpcds_hdfs_parquet_10.db/store_sales/ss_sold_date_sk=2450817/"
        "part-00013-0828d1ab-ef1f-4b55-bf94-c071fb76c353.c000.snappy.parquet";
    
    std::string requiredColumnName = R"(
      {
          "name":"my_schema",
          "fields":[
              {
                  "name":"ss_sold_time_sk",
                  "value":true
              },
              {
                  "name":"ss_sold_time_sk",
                  "value":true
              }
          ]
      }
    )";

    std::string filterJson = R"(
        {
            "FilterTypeName":"and",
            "LeftNode":{
            "FilterTypeName":"lt",
            "ColumnType":"Integer",
            "ColumnName":"ss_sold_time_sk",
            "Value":"28855"
            },
            "RightNode":{
            "FilterTypeName":"gt",
            "ColumnType":"Integer",
            "ColumnName":"ss_item_sk",
            "Value":"3"
            }
        }
      
    )";

    int firstRowGroup = 0;
    int rowGroupToRead = 1;

    ape::Reader test_reader;
    test_reader.init(file_name, hdfs_host, hdfs_port,requiredColumnName, firstRowGroup, rowGroupToRead);
    test_reader.setFilter(filterJson);

    bool res = test_reader.doPredicateFilter(0);
    std::cout<<"predicateFiltere: "<<res<<std::endl;

    test_reader.close();

}