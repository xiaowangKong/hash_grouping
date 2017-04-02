 #include <iostream>
#include "MyHash.h"
#include <fstream>
#include "timer.h"

 using namespace std;
 string inttostring_main(int he){
     char t[256];
     string s;

     sprintf(t, "%d", he);
     s = t;
     return s;
 }
 void hash_grouping(string in_path,string basepath,string respath,uint64_t bin_num,uint64_t sub_file_num,uint64_t buffer_size_B,int EveFileBufferNum){
     MyHash * myhash = new MyHash(bin_num,sub_file_num,buffer_size_B,EveFileBufferNum);//为了测试，先默认10个桶，2个文件，分组缓存100B，每个小源文件缓存10条
     ifstream in;
     //char * in_path ="/home/kxw/ClionProjects/hash_grouping/test_data/pg_50/pg50";
     in.open(in_path.c_str(),ios::in);
     if(!in){
         cout<<"输入文件打不开！"<<endl;
         exit(0);
     }
     string line;
     getline(in,line);
     while(!in.eof()){
         string::size_type index = line.find_first_of('\t',0);
         myhash->insert_kv_lru(line.substr(0,index),line.substr(index+1),basepath,'\t');
         getline(in,line);
     }
     in.close();
     myhash->flush_kvbuffer_tosubfile(basepath,'\t');//basepath split_kv
     myhash->transvert_frequency_to_offset();
     myhash->key_grouping('\t','\n',basepath,
                         respath);//split_kv,split_kv对儿，basepath，respath
     ///走到这一步，源文件已经被分为几个小文件了，文件个数是sub_file_num,频率计数表存在base构成的链表中，
     ////下一步扫描每个小文件，每个小文件涉及的key范围比较小，文件内部key是混乱的，根据内存中频率统计表的位置，
     //myhash.print_res_file(respath);
     delete myhash;
     for(int k =0;k<sub_file_num;k++){
         if(remove((basepath+"/"+inttostring_main(k)).c_str()))  cout<<"删除文件pagerank/0,1,2...失败！"<<endl;
         if(remove((respath+"/"+inttostring_main(k)).c_str()))  cout<<"删除文件pagerank/res/0,1,2...失败！"<<endl;
     }
 }
int main(int argc,char** argv) {
    string in_path ="/home/kxw/研一开题/data/pagerank/pg_message";
    string dir="/home/kxw/研一开题/data/pagerank";
    string out=dir+"/res";
    uint64_t bin_num = 1000000;//设置时写１００
    uint64_t sub_file_num = 10;//设置时写１０
    uint64_t buffer_size_B = 1000000;//设置时写１就是ｍａｐ<key,list_of_value>构成的缓存，积累一定量的ｋｅｙ；ｖａｌｕｅ对，才能写入文件
    int EveFileBufferNum = 50000;//设置时写５//整个源文件划分为多个小源文件时，每个小源文件的缓存，就是往小源文件写的数据先积累一下再写入。
    Timer timer;
    if(argc<8) {
        cout<<"参数个数过少！使用默认参数咯！"<<endl;
        hash_grouping(in_path,dir,out,bin_num,sub_file_num,buffer_size_B,EveFileBufferNum);
    }else {
        dir = argv[1];
        in_path = argv[2];
        out = argv[3];
        bin_num = atoi(argv[4])*10000;//以万为单位
        //  cout<<"bin_num="<<bin_num<<endl;
        sub_file_num = atoi(argv[5]);
        //  cout<<"sub_file_num="<<sub_file_num<<endl;
        buffer_size_B = atoi(argv[6])*1000000; // 以Ｍ为单位
        // cout<<"buffer_size_B="<<buffer_size_B<<endl;
        EveFileBufferNum = atoi(argv[7])*10000;//以万为单位
        // cout<<"EveFileBufferNum="<<EveFileBufferNum<<endl;
        cout<<"参数匹配，开始运行！"<<endl;
        hash_grouping(in_path,dir,out,bin_num,sub_file_num,buffer_size_B,EveFileBufferNum);
    }
   // string basepath = "/home/kxw/ClionProjects/hash_grouping_update/test_data/pg_自己编的/";
    cout<<timer.elapsed()<<endl;
    return 0;
}
