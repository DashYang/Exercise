#include <stdlib.h>
#include <vector>
#include <chrono>

long long GetCurrentTimestamp() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch())
        .count();
}

class DataBlock {
public:
    DataBlock(int begin, int end);
    void Print();
    bool Get(int index, int& value);
    bool Set(int index, const int& value);
    size_t GetSize();

private:
    std::vector<int> array_;
};

using DataBlockPtr = std::shared_ptr<DataBlock>;

class Partition {
public:
    Partition(int begin, int end);
    DataBlockPtr GetDataBlock(int index);
    size_t GetSize();
    void Print();

private:
    std::vector<DataBlockPtr> data_blocks_;
};

using PartitionPtr = std::shared_ptr<Partition>;

class Tester {
public:
    Tester(int min_test_case_number, int max_test_case_number, int min_partition_value,
           int max_partition_value);
    // random generate partition datas
    void GenerateTestCase(int partition_number, std::vector<PartitionPtr>& test_case);

private:
    // get an value from test case by 1. partition 2 data block 3 index. if return false, we need
    // loop until value is found
    bool GetValueByPDI(const std::vector<PartitionPtr>& test_case, int& partition_index,
                       int& data_block_index, int& value_index, int& value);

    // sort partitions
    bool MergeSort(const std::vector<PartitionPtr>& test_case, int begin, int mid, int end);
    bool MergeSortList(const std::vector<int>& input, std::vector<int>& output, int begin, int mid, int end);
    void MakeSortListToPartition(const int& begin, const int&end, const std::vector<int>& sort_list, std::vector<PartitionPtr> test_case, int total_cnt = 0);

public:
    // sort these partitions
    void SortPartitions(std::vector<PartitionPtr>& test_case, bool parallel = false);
    void HeapSortPartitions(std::vector<PartitionPtr>& test_case, bool pre_resize);
    void ParallelMergeSortWithoutCopy(std::vector<PartitionPtr>& test_case);
    bool CheckResult(std::vector<PartitionPtr>& test_case);
    void RunTestCase();
    void PrintResult();
    void PrintTestCase(std::vector<PartitionPtr>& test_case);

private:
    void MergeSort(PartitionPtr a, PartitionPtr b);
    int min_test_case_number_ = 1;
    int max_test_case_number_ = 4;
    int min_partition_value_ = 1;
    int max_partition_value_ = 100;
    std::vector<std::vector<PartitionPtr>> failed_test_cases_;
};