//
// Created by kxw on 1/15/17.
//

#ifndef HASH_GROUPING_MYHASH_H
#define HASH_GROUPING_MYHASH_H


#include <stdint-gcc.h>
#include <string>
#include <cstdlib>
#include <vector>
#include "KVBuffer.h"
using namespace std;
struct list_node{
    string k;
    uint64_t fk_size;
    vector<string> listofv;//将第二层缓存放入hash表，初始时为null
   list_node* next;
    list_node(){
        fk_size=0;
        next = NULL;
    }
};
struct base_node{
    uint64_t fk_sum;
    list_node* next;
    base_node(){
        fk_sum=0;
        next=NULL;
    }
};

class MyHash {
public:
    base_node* base;//用来存hash桶的基数组
    uint64_t bin_num;//一共分多少个桶
    uint64_t sub_file_num;//将源文件分成几个小文件
    KVBuffer* kvbuffer;
    uint64_t buffer_size_B;
    uint64_t current_size_B;
    MyHash(uint64_t bin_num, uint64_t sub_file_num,uint64_t  buffer_size_B,int EveFileBufferNum);//初始化，猪妖完成base数组开辟空间
    uint64_t get_hash_id(string k,const unsigned int seed);
    uint64_t get_file_id(string k , const unsigned int seed);
    bool insert_kv_sort(string k,string v,const string base_path,char split);
    bool insert_kv_lru(string k,string v,const string base_path,char split_kv);
    int flush_kvbuffer_tosubfile(string basepath,char split_kv);
    int transvert_frequency_to_offset();
    void delete_hash_linkedlist();//遍历频率统计表，释放所有链表节点的空间
    int flushtofile(string basepath,char split_kv,char split_vv,int sub_file);//传递一个判别每个key所属的文件的函数
    int  key_grouping(char split_kv,char split_vv,string basepath,string outpath);///扫描小源文件，查hash表根据位置将数据插入文件，其中还要构造一个buffer累计各个key，多了再进行插入
    int print_res_file(string outpath);
    ///下面是key_grouping的Groupingbuffer相关函数
    bool full_or_not(uint64_t needToInsertKV_inB);
    int insert(string k,string v,char split_vv);
    ~MyHash();
};


#endif //HASH_GROUPING_MYHASH_H
