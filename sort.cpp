#include "sort.h"
#include <time.h>
#include <iostream>
#include <map>
#include <thread>
#include "thread_pool.cpp"

DataBlock::DataBlock(int begin, int end) {
    if (begin > end) {
        std::cout << "invalid parameter for data block" << std::endl;
        return;
    }

    while (begin <= end) {
        int next = begin + (rand() % (end - begin + 1));
        array_.push_back(next);
        begin = next + 1;
    }
}

void DataBlock::Print() {
    for (int& item : array_) {
        std::cout << item << " ";
    }
    std::cout << std::endl;
}

bool DataBlock::Get(int index, int& value) {
    if (index < 0 || index >= array_.size()) return false;
    value = array_[index];
    return true;
}

bool DataBlock::Set(int index, const int& value) {
    if (index < 0 || index >= array_.size()) return false;
    array_[index] = value;
    return true;
}

size_t DataBlock::GetSize() { return array_.size(); }

Partition::Partition(int begin, int end) {
    if (begin > end) {
        std::cout << "invalid parameter for data block" << std::endl;
        return;
    }

    while (begin <= end) {
        int next = begin + (rand() % (end - begin + 1));
        // std::cout << "make data block from begin:" << begin << " end:" << next << std::endl;
        DataBlockPtr data_block = std::make_shared<DataBlock>(begin, next);
        data_blocks_.push_back(data_block);
        begin = next + 1;
    }
}

DataBlockPtr Partition::GetDataBlock(int index) {
    if (index < 0 || index >= data_blocks_.size()) {
        return nullptr;
    }
    return data_blocks_[index];
}

size_t Partition::GetSize() { return data_blocks_.size(); }

void Partition::Print() {
    for (DataBlockPtr& data_block : data_blocks_) {
        data_block->Print();
    }
    std::cout << std::endl;
}

Tester::Tester(int min_test_case_number, int max_test_case_number, int min_partition_value,
               int max_partition_value) {
    min_test_case_number_ = min_test_case_number;
    max_test_case_number_ = max_test_case_number;
    min_partition_value_ = min_partition_value;
    max_partition_value_ = max_partition_value;
}

// random generate partition datas
void Tester::GenerateTestCase(int partition_number, std::vector<PartitionPtr>& test_case) {
    test_case.clear();
    // int number = rand() % max_test_case_number_;
    int number = partition_number;
    for (int i = 0; i < number; i++) {
        PartitionPtr par = std::make_shared<Partition>(min_partition_value_, max_partition_value_);
        test_case.push_back(par);
    }
    std::cout << "generate " << number << " partitions" << std::endl;
}

bool Tester::GetValueByPDI(const std::vector<PartitionPtr>& test_case, int& partition_index,
                           int& data_block_index, int& value_index, int& value) {
    if (test_case[partition_index]->GetSize() <= data_block_index) {
        partition_index++;
        data_block_index = 0;
        value_index = 0;
        return false;
    }

    DataBlockPtr data_block = test_case[partition_index]->GetDataBlock(data_block_index);
    if (!data_block) {
        data_block_index++;
        value_index = 0;
        return false;
    }

    size_t total_number = data_block->GetSize();
    if (total_number <= value_index) {
        data_block_index++;
        value_index = 0;
        return false;
    }

    // check condition before, it is always true
    data_block->Get(value_index, value);
    // std::cout << "get value from " << partition_index << " " << data_block_index << " "
    //           << value_index << " " << value << std::endl;
    return true;
}

bool Tester::MergeSort(const std::vector<PartitionPtr>& test_case, int begin, int mid, int end) {
    size_t case_size = test_case.size();
    if (begin < 0 || end >= case_size) {
        std::cout << "invalid parameter " << begin << " " << mid << " " << end << std::endl;
        return false;
    }
    if (begin > mid || mid + 1 > end) {
        std::cout << "invalid parameter " << begin << " " << mid << " " << end << std::endl;
        return false;
    }
    if (begin == mid && mid == end) {
        return true;
    }
    std::vector<int> sort_list;
    int first_begin_partition = begin;
    int second_begin_partition = mid + 1;
    int first_data_block_index = 0, second_data_block_index = 0;
    int first_index = 0, second_index = 0;
    auto start_time = GetCurrentTimestamp();
    int cnt = 0;
    while (true) {
        if (first_begin_partition > mid || second_begin_partition > end) {
            break;
        }
        cnt++;

        int first_value = min_partition_value_;
        int second_value = min_partition_value_;

        if (!GetValueByPDI(test_case, first_begin_partition, first_data_block_index, first_index,
                           first_value)) {
            continue;
        }

        if (!GetValueByPDI(test_case, second_begin_partition, second_data_block_index, second_index,
                           second_value)) {
            continue;
        }

        if (first_value <= second_value) {
            sort_list.push_back(first_value);
            first_index++;
        } else {
            sort_list.push_back(second_value);
            second_index++;
        }
    }
    auto end_time = GetCurrentTimestamp();
    // std::cout << " two merge" << end_time - start_time << " cnt " << cnt << std::endl;

    while (first_begin_partition <= mid) {
        int first_value = min_partition_value_;

        if (!GetValueByPDI(test_case, first_begin_partition, first_data_block_index, first_index,
                           first_value)) {
            continue;
        }
        sort_list.push_back(first_value);
        first_index++;
    }

    while (second_begin_partition <= end) {
        int second_value = min_partition_value_;

        if (!GetValueByPDI(test_case, second_begin_partition, second_data_block_index, second_index,
                           second_value)) {
            continue;
        }
        sort_list.push_back(second_value);
        second_index++;
    }
    end_time = GetCurrentTimestamp();
    // std::cout << "single merge " << end_time - start_time << std::endl;

    start_time = GetCurrentTimestamp();
    // make sort list to partitions
    MakeSortListToPartition(begin, end, sort_list, test_case);
    // std::cout << std::endl;
    end_time = GetCurrentTimestamp();
    // std::cout << "make sort to partition " << end_time - start_time << std::endl;
    return true;
}

bool Tester::MergeSortList(const std::vector<int>& input, std::vector<int>& output, int begin,
                           int mid, int end) {
    if (begin < 0) {
        std::cout << "invalid parameter " << begin << " " << mid << " " << end << std::endl;
        return false;
    }
    if (begin > mid || mid + 1 > end) {
        std::cout << "invalid parameter " << begin << " " << mid << " " << end << std::endl;
        return false;
    }
    if (begin == mid && mid == end) {
        return true;
    }
    int output_index = begin;
    int first_begin = begin, second_begin = mid + 1;
    while (first_begin <= mid && second_begin <= end) {
        if (input[first_begin] <= input[second_begin]) {
            output[output_index++] = input[first_begin++];
        } else {
            output[output_index++] = input[second_begin++];
        }
    }

    while (first_begin <= mid) {
        output[output_index++] = input[first_begin++];
    }

    while (second_begin <= end) {
        output[output_index++] = input[second_begin++];
    }

    // for (int i = 0; i < output_index; i++) {
    //     printf("%d ", output[i]);
    // }
    // printf("\n");

    // std::cout << "after merge sort " << output_index << " " << end << std::endl;
    return true;
}

void Tester::MakeSortListToPartition(const int& begin, const int& end,
                                     const std::vector<int>& sort_list,
                                     std::vector<PartitionPtr> test_case, int total_cnt) {
    int sort_list_size = sort_list.size();
    if (total_cnt != 0) {
        sort_list_size = total_cnt;
    }
    int partition_index = begin;
    int data_block_index = 0;
    int value_index = 0;
    int sort_index = 0;
    while (true) {
        if (sort_index >= sort_list_size) {
            break;
        }
        if (partition_index > end) {
            std::cout << "fatal error, over the range" << std::endl;
            break;
        }
        PartitionPtr partition = test_case[partition_index];
        if (!partition) {
            std::cout << "nullptr" << std::endl;
            break;
        }
        if (data_block_index >= partition->GetSize()) {
            partition_index++;
            data_block_index = 0;
            value_index = 0;
            continue;
        }

        DataBlockPtr data_block = partition->GetDataBlock(data_block_index);
        if (!data_block) {
            std::cout << "nullptr" << std::endl;
            break;
        }
        if (value_index >= data_block->GetSize()) {
            data_block_index++;
            value_index = 0;
            continue;
        }

        // std::cout << sort_list[sort_index] << " ";
        data_block->Set(value_index, sort_list[sort_index]);
        int value = min_partition_value_;
        // data_block->Get(value_index, value);
        // std::cout << "set value " << partition_index << " " << data_block_index << " "
        //           << value_index << " " << value << std::endl;
        value_index++;
        sort_index++;
    }
}

void Tester::SortPartitions(std::vector<PartitionPtr>& test_case, bool parallel) {
    int test_case_size = test_case.size();
    if (test_case_size <= 1) {
        return;
    }
    int range = 1;

    ThreadPoolPtr thread_pool = nullptr;
    int threads = std::thread::hardware_concurrency();
    if (parallel) {
        thread_pool = std::make_shared<ThreadPool>(threads);
        std::cout << "threads number " << threads << std::endl;
    }
    while (range < test_case_size) {
        int todo_cnt = 0;
        std::atomic<int> do_cnt(0);

        auto start_time = GetCurrentTimestamp();
        for (int begin = 0; begin < test_case_size - range; begin += 2 * range) {
            int task_number = test_case_size / (2 * range);
            int end = begin + range * 2 - 1;
            if (end >= test_case_size) {
                end = test_case_size - 1;
            }
            int mid = begin + range - 1;
            // std::cout << "merge " << begin << " " << mid << " " << end << std::endl;
            // single thread merge sort
            if (!parallel) {
                auto start_time = GetCurrentTimestamp();
                if (!MergeSort(test_case, begin, mid, end)) {
                    std::cout << "merge sort error " << std::endl;
                }
                auto end_time = GetCurrentTimestamp();
                // std::cout << "serial " << begin << " "
                //           << (end_time - start_time)<< " ms"
                //           << std::endl;

                // thread pool to handle merge sort
            } else if (thread_pool /** && task_number <= threads **/) {
                // add task to thread pool
                todo_cnt++;
                thread_pool->enqueue([this, &test_case, begin, mid, end, &do_cnt]() {
                    auto start_time = GetCurrentTimestamp();
                    MergeSort(test_case, begin, mid, end);
                    auto end_time = GetCurrentTimestamp();
                    do_cnt++;
                    // printf("parallel %d %llu ms\n", do_cnt.load(), (end_time - start_time));
                });
            } else {
                auto start_time = GetCurrentTimestamp();
                if (!MergeSort(test_case, begin, mid, end)) {
                    std::cout << "merge sort error " << std::endl;
                }
                auto end_time = GetCurrentTimestamp();
                // std::cout << "task too large, serial " << begin << " "
                //           << (end_time - start_time) << " ms"
                //           << std::endl;
            }
        }
        auto end_time = GetCurrentTimestamp();
        // std::cout << "serial merge range " << range << " "
        //           << (end_time - start_time)<< "ms." << std::endl;

        if (parallel) {
            while (do_cnt.load() < todo_cnt)
                ;
        }
        end_time = GetCurrentTimestamp();
        // std::cout << "parallel merge range " << range << " "
        //           << (end_time - start_time)<< "ms." << std::endl;
        range *= 2;
    }
}

void Tester::HeapSortPartitions(std::vector<PartitionPtr>& test_case, bool pre_resize) {
    if (test_case.size() <= 1) return;

    // the first one is the index of partition, the second one is value
    auto cmp = [](std::pair<int, int> a, std::pair<int, int> b) { return a.second > b.second; };
    std::priority_queue<std::pair<int, int>, std::vector<std::pair<int, int>>, decltype(cmp)>
        waitting_queue(cmp);
    std::vector<int> sort_list;

    auto start_time = GetCurrentTimestamp();
    if (pre_resize) {
        sort_list.reserve(1000000000);
    }
    auto end_time = GetCurrentTimestamp();
    // std::cout << end_time - start_time << std::endl;
    int total_cnt = 0;

    std::map<int, std::pair<int, int>> index_map;
    for (int i = 0; i < test_case.size(); i++) {
        if (test_case[i]->GetSize() > 0 && test_case[i]->GetDataBlock(0)->GetSize() > 0) {
            int value = 0;
            test_case[i]->GetDataBlock(0)->Get(0, value);
            waitting_queue.push(std::make_pair(i, value));
            index_map.insert({i, std::make_pair(0, 0)});
        }
    }

    while (!waitting_queue.empty()) {
        auto top = waitting_queue.top();
        waitting_queue.pop();
        if (top.second == max_partition_value_ + 1) {
            break;
        }
        // std::cout << "pop " << top.second << " " << top.first << std::endl;
        if (!pre_resize) {
            sort_list.push_back(top.second);
        } else {
            sort_list[total_cnt] = top.second;
        }
        total_cnt++;

        // get next partition value
        int partition_id = top.first;
        PartitionPtr partition = test_case[partition_id];
        auto location = index_map[partition_id];
        int data_block_index = location.first;
        int index = location.second + 1;
        int next_value = max_partition_value_ + 1;
        while (true) {
            if (data_block_index >= partition->GetSize()) {
                waitting_queue.push(std::make_pair(partition_id, next_value));
                break;
            } else {
                if (index >= partition->GetDataBlock(data_block_index)->GetSize()) {
                    data_block_index++;
                    index = 0;
                } else {
                    partition->GetDataBlock(data_block_index)->Get(index, next_value);
                    index_map[partition_id] = std::make_pair(data_block_index, index);
                    waitting_queue.push(std::make_pair(partition_id, next_value));
                    break;
                }
            }
        }
    }

    // std::cout << "total cnt " << total_cnt << std::endl;
    MakeSortListToPartition(0, test_case.size() - 1, sort_list, test_case, total_cnt);
}

void Tester::ParallelMergeSortWithoutCopy(std::vector<PartitionPtr>& test_case) {
    std::vector<std::vector<int>> double_buffer;
    std::vector<int> single_buffer;
    double_buffer.push_back(single_buffer);
    double_buffer.push_back(single_buffer);
    double_buffer[0].reserve(1000000000);
    double_buffer[1].reserve(1000000000);

    std::vector<std::pair<int, int>> partition_range;
    partition_range.reserve(test_case.size());
    int input_index = 0;
    int partition_id = 0;
    int total_cnt = 0;
    for (auto& item : test_case) {
        int data_block_size = item->GetSize();
        int begin_index = input_index;
        for (int i = 0; i < data_block_size; i++) {
            int value_size = item->GetDataBlock(i)->GetSize();
            for (int j = 0; j < value_size; j++) {
                int value;
                item->GetDataBlock(i)->Get(j, value);
                // std::cout << "value " << value << std::endl;
                double_buffer[0][input_index++] = value;
                total_cnt++;
            }
        }
        int end_index = input_index - 1;
        if (end_index >= begin_index) {
            partition_range[partition_id++] = std::make_pair(begin_index, end_index);
        }
        // std::cout << begin_index << " " << end_index << std::endl;
    }

    int range = 1;
    ThreadPoolPtr thread_pool = nullptr;
    int threads = std::thread::hardware_concurrency();
    thread_pool = std::make_shared<ThreadPool>(threads);
    std::cout << "threads number " << threads << std::endl;

    int test_case_size = partition_id;
    int merge_cnt = 0;
    while (range < test_case_size) {
        int todo_cnt = 0;
        std::atomic<int> do_cnt(0);

        auto start_time = GetCurrentTimestamp();
        int begin = 0;
        for (; begin < test_case_size - range; begin += 2 * range) {
            int task_number = test_case_size / (2 * range);
            int end = begin + range * 2 - 1;
            if (end >= test_case_size) {
                end = test_case_size - 1;
            }
            int mid = begin + range - 1;
            // std::cout << begin << " " << mid << " " << end << std::endl;

            todo_cnt++;
            int begin_index = partition_range[begin].first;
            int mid_index = partition_range[mid].second;
            int end_index = partition_range[end].second;
            // std::cout << "begin " << begin_index << " " << mid_index << " " << end_index
            //           << std::endl;
            thread_pool->enqueue(
                [this, &double_buffer, merge_cnt, begin_index, mid_index, end_index, &do_cnt]() {
                    auto start_time = GetCurrentTimestamp();
                    MergeSortList(double_buffer[merge_cnt % 2], double_buffer[(merge_cnt + 1) % 2],
                                  begin_index, mid_index, end_index);
                    do_cnt++;
                });
        }

        // copy left to the rightmost
        if (begin < test_case_size) {
            int begin_index = partition_range[begin].first;
            int end_index = partition_range[test_case_size - 1].second;
            for (int i = begin_index; i <= end_index; i++) {
                double_buffer[(merge_cnt + 1) % 2][i] = double_buffer[merge_cnt % 2][i];
            }
        }
        auto end_time = GetCurrentTimestamp();
        // std::cout << "serial merge range " << range << " " << (end_time - start_time) << "ms."
        //           << std::endl;

        while (do_cnt.load() < todo_cnt)
            ;

        merge_cnt++;
        end_time = GetCurrentTimestamp();
        // std::cout << "parallel merge range " << range << " "
        //           << (end_time - start_time)<< "ms." << std::endl;
        range *= 2;
    }

    MakeSortListToPartition(0, test_case.size() - 1, double_buffer[merge_cnt % 2], test_case,
                            total_cnt);
}

bool Tester::CheckResult(std::vector<PartitionPtr>& test_case) {
    int last_number = min_partition_value_;  // the min number can appear int partition
    for (PartitionPtr& par : test_case) {
        size_t partition_size = par->GetSize();
        for (int i = 0; i < partition_size; i++) {
            DataBlockPtr data_block = par->GetDataBlock(i);
            if (!data_block) {
                std::cout << "nullptr" << std::endl;
                return false;
            }
            size_t data_block_size = data_block->GetSize();
            for (int j = 0; j < data_block_size; j++) {
                int value = 0;
                if (data_block->Get(j, value)) {
                    if (value < last_number) {
                        std::cout << "last_number=" << last_number << " value=" << value
                                  << std::endl;
                        return false;
                    }
                    last_number = value;
                } else {
                    std::cout << "fatal error: can't find value" << std::endl;
                    return false;
                }
            }
        }
    }
    return true;
}

void Tester::RunTestCase() {
    for (int partition_number = min_test_case_number_; partition_number <= max_test_case_number_;
         partition_number++) {
        std::vector<PartitionPtr> test_case;

        long long start = 0, end = 0;
        /***
        // 1. parallel merge sort
        GenerateTestCase(partition_number, test_case);
        // std::cout << "before sort" << std::endl;
        // PrintTestCase(test_case);
        start = GetCurrentTimestamp();
        // SortPartitions(test_case, false);
        SortPartitions(test_case, true);
        end = GetCurrentTimestamp();
        // std::cout << "after sort" << std::endl;
        // PrintTestCase(test_case);
        if (!CheckResult(test_case)) {
            failed_test_cases_.push_back(test_case);
        } else {
            std::cout << "parallel pass " << partition_number << " test case! " << (end - start)
                      << "ms." << std::endl;
        }

        // 2. single thread merge sort
        GenerateTestCase(partition_number, test_case);
        // std::cout << "before sort" << std::endl;
        // PrintTestCase(test_case);
        start = GetCurrentTimestamp();
        // SortPartitions(test_case, false);
        SortPartitions(test_case, false);
        end = GetCurrentTimestamp();
        // std::cout << "after sort" << std::endl;
        // PrintTestCase(test_case);
        if (!CheckResult(test_case)) {
            failed_test_cases_.push_back(test_case);
        } else {
            std::cout << "single thread pass " << partition_number << " test case! "
                      << (end - start) << "ms." << std::endl;
        }

        // 3. heap sortition no pre_resize
        GenerateTestCase(partition_number, test_case);
        // std::cout << "before sort" << std::endl;
        // PrintTestCase(test_case);
        start = GetCurrentTimestamp();
        // SortPartitions(test_case, false);
        HeapSortPartitions(test_case, false);
        end = GetCurrentTimestamp();
        // std::cout << "after sort" << std::endl;
        // PrintTestCase(test_case);
        if (!CheckResult(test_case)) {
            failed_test_cases_.push_back(test_case);
        } else {
            std::cout << "no pre-resize heap sortion pass " << partition_number << " test case! "
                      << (end - start) << "ms." << std::endl;
        }
        **/

        // 4. heap sortition pre_resize
        GenerateTestCase(partition_number, test_case);
        // std::cout << "before sort" << std::endl;
        // PrintTestCase(test_case);
        start = GetCurrentTimestamp();
        // SortPartitions(test_case, false);
        HeapSortPartitions(test_case, true);
        end = GetCurrentTimestamp();
        // std::cout << "after sort" << std::endl;
        // PrintTestCase(test_case);
        if (!CheckResult(test_case)) {
            failed_test_cases_.push_back(test_case);
        } else {
            std::cout << "pre-resize heap sortion pass " << partition_number << " test case! "
                      << (end - start) << "ms." << std::endl;
        }

        // 5. heap sortition pre_resize
        GenerateTestCase(partition_number, test_case);
        // std::cout << "before sort" << std::endl;
        // PrintTestCase(test_case);
        start = GetCurrentTimestamp();
        ParallelMergeSortWithoutCopy(test_case);
        end = GetCurrentTimestamp();
        // std::cout << "after sort" << std::endl;
        // PrintTestCase(test_case);
        if (!CheckResult(test_case)) {
            failed_test_cases_.push_back(test_case);
        } else {
            std::cout << "parallel heap sortion pass " << partition_number << " test case! "
                      << (end - start) << "ms." << std::endl;
        }
    }
}

void Tester::PrintResult() {
    if (failed_test_cases_.size() == 0) {
        std::cout << "all case passed" << std::endl;
    } else {
        std::cout << "following " << failed_test_cases_.size() << " test case didn't passed"
                  << std::endl;
        for (auto& test_case : failed_test_cases_) {
            std::cout << "case" << std::endl;
            for (auto& partition : test_case) {
                partition->Print();
            }
        }
    }
}

void Tester::PrintTestCase(std::vector<PartitionPtr>& test_case) {
    for (auto& partition : test_case) {
        partition->Print();
    }
}

int main(int argc, char* argv[]) {
    if (argc < 5) {
        std::cout << "please input ./main [min_test_case_number] [max_test_case_number] "
                     "[min_partition_value] "
                     "[max_partition_value] "
                  << std::endl;
        return 0;
    }
    int min_test_case_number = atoi(argv[1]);
    int max_test_case_number = atoi(argv[2]);
    int min_partition_value = atoi(argv[3]);
    int max_partition_value = atoi(argv[4]);

    if (min_test_case_number > max_test_case_number || min_partition_value > max_partition_value) {
        std::cout << "invalid parameter, min should be less than max" << std::endl;
        return 0;
    }

    if (min_test_case_number < 0 || max_test_case_number < 0 || min_partition_value < 0 ||
        max_partition_value < 0) {
        std::cout << "invalid parameter, not support for negative parameter" << std::endl;
        return 0;
    }

    Tester t(min_test_case_number, max_test_case_number, min_partition_value, max_partition_value);
    t.RunTestCase();

    // output failed test case
    t.PrintResult();
    return 0;
}