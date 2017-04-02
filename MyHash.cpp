//
// Created by kxw on 1/15/17.
//

#include "MyHash.h"
#include "murmurhash.h"
#include <fstream>
#include <vector>
MyHash::MyHash(uint64_t bin_num, uint64_t sub_file_num, uint64_t buffer_size_B,int EveFileBufferNum) {
    this->buffer_size_B = buffer_size_B;/////????如何在一个类里创建另一个类的对象作为成员，并且调用构造函数
    this->current_size_B = 0;
    this->bin_num = bin_num;
    this->sub_file_num = sub_file_num;
    base = new base_node[bin_num];
    ////下面是源文件分割成小文件的缓存！
    kvbuffer = new KVBuffer(sub_file_num,EveFileBufferNum);
    //
}
uint64_t MyHash::get_hash_id(string k,const unsigned int seed) {
    uint64_t murmur_id = MurmurHash64A (k.c_str(), k.size(), seed);
    uint64_t bin_id = murmur_id%bin_num;//查看映射到哪个桶中
    return bin_id;
}
uint64_t MyHash::get_file_id(string k, const unsigned int seed) {
   return  get_hash_id(k,seed)/(bin_num/sub_file_num);
   // cout<<"傻！"<<endl;
}
bool MyHash::insert_kv_sort(string k, string v,string base_path,char split) {//参数的意思分别为要插入的key、value、子文件路经、kv分隔符
    unsigned int seed = 59;//通常是一个质数，所有hash用同一个种子
    uint64_t bin_id  = get_hash_id(k,seed);
    cout<<base+bin_id<<endl;
    (base+bin_id)->fk_sum+=(v.size()+1+k.size()+1);///一个value的大小再加上1,再加上k的大小再加上1
    if((base+bin_id)->next==NULL){//说明是第一个节点
      //  struct list_node  current_strut;    如果想保留退出本函数还存在的变量值，不要在栈上分配空间！，在堆上分配空间
        //struct list_node * current =& current_strut;
        struct list_node * current =new list_node ;//结构体数组动态分配
        current->k=k;
        current->fk_size=v.size()+1+k.size()+1;//获取字符串长度，每个字符占一个字节？
        current->next=NULL;
        (base+bin_id)->next = current;//将第一个节点插入base中
        cout<<"current = "<<current<<endl;
    }else{
        list_node * pt =  base[bin_id].next,*prev = pt;
        while(pt!=NULL&&pt->k.compare(k)<0){//pt的key小于k
            prev = pt;
            pt=pt->next;
        }//跳出循环的话说明走到链表尾部，说明pt的key大于等于当前key
        if(pt!=NULL&&(pt->k.compare(k)==0)){//pt的key等于k
            pt->fk_size+=k.size()+1+v.size()+1;//找到当前key对应的频率统计节点，将频率增加
        }
        else if(pt!=NULL&&(pt->k.compare(k)>0)){//pt的key大于key  ,需要注意还有可能是在base与listnode之间插入
            struct list_node * current = new list_node;
            current->k=k;
            current->fk_size=v.size()+1+k.size()+1;//获取字符串长度，每个字符占一个字节？
            if(prev==pt){
                current->next=pt;
                (base+bin_id)->next = current;//插入到base节点和第一个listnode之间，成为新的第一个listnode
            }
            else{
                current->next=pt;
                prev->next = current;//然后再将比新节点小的prev的next节点指向新节点}
            }//先将新节点的next、指向比它大的节点pt


        }else{//说明呢，pt为null，也就是最后一个节点的key值都小于k，则应插入到链表结尾
            struct list_node * current = new list_node;
            current->k=k;
            current->fk_size=v.size()+1+k.size()+1;//获取字符串长度，每个字符占一个字节？
            current->next=NULL;
            prev->next=current;
        }
    }

    ///追加写入kv到对应文件
     int file_id=(unsigned int)(bin_id%sub_file_num);
    ofstream out;
    out.open((base_path+"/"+inttostring(file_id)).c_str(),ios::app|ios::ate);
    if(!out){
        cout<<"文件"<<base_path<<"/"<<file_id<<"打开失败！"<<endl;
        exit(0);
    }
    out<<k<<split<<v<<endl;//将kv对写入sub文件
    out.close();
    ///追加写入kv到对应文件
    return true;
}
bool MyHash::insert_kv_lru(string k, string v,string base_path,char split_kv) {//参数的意思分别为要插入的key、value、子文件路经、kv分隔符
    unsigned int seed = 59;//通常是一个质数，所有hash用同一个种子
    uint64_t bin_id  = get_hash_id(k,seed);
    //cout<<base+bin_id<<endl;
    (base+bin_id)->fk_sum+=(v.size()+1+k.size()+1);///一个value的大小再加上1
    if((base+bin_id)->next==NULL){//说明是第一个节点
        //  struct list_node  current_strut;    如果想保留退出本函数还存在的变量值，不要在栈上分配空间！，在堆上分配空间
        //struct list_node * current =& current_strut;
        struct list_node * current =new list_node ;//结构体数组动态分配
        current->k=k;
        current->fk_size=v.size()+1+k.size()+1;//获取字符串长度，每个字符占一个字节？
        //(base+bin_id)->fk_sum+=k.size()+1;
        current->next=NULL;
        (base+bin_id)->next = current;//将第一个节点插入base中
       // cout<<"current = "<<current<<endl;
    }else{
        list_node * pt =  base[bin_id].next,*prev = pt;
        while(pt!=NULL&&pt->k.compare(k)!=0){//pt的key小于k
            prev = pt;
            pt=pt->next;
        }//跳出循环的话说明走到链表尾部，说明pt的key大于等于当前key
        if(pt!=NULL&&(pt->k.compare(k)==0)){//pt的key等于k
            pt->fk_size+=(v.size()+1+k.size()+1);//找到当前key对应的频率统计节点，将频率增加
            ///然后将当前节点从当前位置删除，插入到头节点处
            if(base[bin_id].next!=pt) {//如果当前修改的节点就是头节点，那么不用再插入了
                prev->next = pt->next;
                pt->next = base[bin_id].next;
                base[bin_id].next = pt;
            }
        }
        else {//pt为空，说明当前key节点还没建立
            struct list_node * current = new list_node;
            current->k=k;
            current->fk_size=v.size()+1+k.size()+1;//获取字符串长度，每个字符占一个字节？
            ////最近使用的插入头节点
            current->next=base[bin_id].next;
            base[bin_id].next = current;
            ///说明是第一次遇见key，则文件里也需要为key预留空间，则当前桶的总预留空间加上一个key的大小再加上1
           // (base+bin_id)->fk_sum+=(k.size()+1);
        }
    }
    //////////下面是为了调试所用，输出当前kv映射到的那个桶的链表////////////////
   /* cout<<"base["<<bin_id<<"]";
    list_node * pt =  base[bin_id].next;
    while(pt!=NULL){//pt的key小于k
        cout<<"->"<<pt->k<<","<<pt->fk_size;
        pt=pt->next;
    }
    cout<<" sum = "<<base[bin_id].fk_sum<<endl;*/
    //////////下面是为了调试所用，输出当前kv映射到的那个桶的链表////////////////
    ///追加写入kv到对应文件
    int file_id= get_file_id(k,seed);
    /*ofstream out;
    out.open((base_path+"/"+inttostring(file_id)).c_str(),ios::app|ios::ate);
    if(!out){
        cout<<"文件"<<base_path<<"/"<<file_id<<"打开失败！"<<endl;
        exit(0);
    }
    out<<k<<split_kv<<v<<endl;//将kv对写入sub文件
    out.close();*/
    ///追加写入kv到对应文件

    //之后又加入缓存，将kv对追加到文件中去
    kvbuffer->insert(k,v,file_id,base_path,split_kv);
    //之后又加入缓存，将kv对追加到文件中去
    return true;
}
int MyHash::flush_kvbuffer_tosubfile(string basepath, char split_kv) {
    kvbuffer->flushReminderToFile(basepath,split_kv);//与insert_kv_lru后两个参数保持一致
    delete kvbuffer;///调用KVBuffer，释放掉占据的内存空间，之后不再需要了！
}
int MyHash::transvert_frequency_to_offset() {
    ///本函数的目的就是将统计好的key频率变成在文件中的offset
    ////遍历base数组，的所有链表元素
//    cout<<"transvert_frequency_to_offset**************************"<<endl;
    int  range = (int)(bin_num/sub_file_num);//每个文件存range个桶里的数据
    for(int i=0;i<sub_file_num;i++){
        int end_id = (i+1)*range;
        uint64_t last_offset = 0;
        for(int j=i*range;j<end_id;j++){///定位到每一个桶，下面要遍历链表
           list_node * ptr = base[j].next;//得到这个桶的第一个链表节点
            while(ptr!=NULL){
                uint64_t current_fk_size = ptr->fk_size;//有待修正，也就是当前这一行我要存(key分隔符)value分割符value分隔符value...
                ptr->fk_size = last_offset;
                last_offset = last_offset+current_fk_size;

                ptr=ptr->next;//处理下一个节点
                /////每个链表节点代表一个key完事儿，也就是说一个key的处理完了，应该加个换行符号，是不？我觉得是，并且这些value之间也应该加个换行符号
            }
            base[j].fk_sum = 0;//使用完了当前这个桶总字节数，就将其置0，方便下次缓存kv时用来保存缓存的大小。
            // 因为last_offset这个变量已经记录了下一次的偏移位置应该不会影响，偏移量计算！！！！！！
            //////////下面是为了调试所用，输出当前kv映射到的那个桶的链表////////////////
        /*    cout<<"base["<<j<<"]";
            list_node * pt =  base[j].next;
            while(pt!=NULL){//pt的key小于k
                cout<<"->"<<pt->k<<","<<pt->fk_size;
                pt=pt->next;
            }
            cout<<" sum = "<<base[j].fk_sum<<endl;*/
            //////////下面是为了调试所用，输出当前kv映射到的那个桶的链表////////////////
        }
    }
    return 0;
}
int MyHash::flushtofile(string basepath, char split_kv,char split_vv,int sub_file) {//当要处理文件sub_file
        if(current_size_B == 0) return 0;//如果没有可以写的kv缓存对就直接返回
        int bin_beg = bin_num/sub_file_num*sub_file;//起始的bin_id
        int bin_end = bin_beg+bin_num/sub_file_num;//终止的bin_id
      fstream out;
      out.open((basepath+"/"+inttostring(sub_file)).c_str(),ios::out|ios::in);//以二进制形式
     if(!out){
        ofstream hehe;
        hehe.open((basepath+"/"+inttostring(sub_file)).c_str());
        hehe.close();
        //cout<<"要写入分组文件："<<basepath+"/"+inttostring(file_id)<<"打不开！"<<endl;
        //exit(0);
        out.open((basepath+"/"+inttostring(sub_file)).c_str(),ios::out|ios::in);//以二进制形式
        if(!out){
            cout<<"要写入分组文件111："<<basepath+"/"+inttostring(sub_file)<<"打不开！"<<endl;
            exit(0);
        }
    }
    uint64_t  last_offset=0;
    for(int i=0;i<bin_end;i++){
            if(base[i].fk_sum >0) {//这个桶中有可以刷新的缓存kv对
               // cout<<""<<endl;
                list_node *ptr = base[i].next;//获取第一个要刷新的节点
                while(ptr!=NULL&& base[i].fk_sum>0){//说明有下一个节点，并且经过统计下一个节点还有剩余kv对被缓存
                    if(!ptr->listofv.empty()){//说明当前这个节点有缓存kv
                        out.seekp(ptr->fk_size-last_offset,ios::cur);
                        //vector<string> split_v = splitByMyChar_returnVectorWithoutDimension(ptr->listofv,split_vv);
                        for(int j=0;j<ptr->listofv.size();j++){
                            out <<ptr->k<< split_kv << ptr->listofv[j] <<split_vv;//形式就是key kv分隔符 value 换行
                            out.flush();
                            ptr->fk_size += ptr->k.size() + 1 + ptr->listofv[j].size() + 1;//追加了kvg
                            base[i].fk_sum = base[i].fk_sum-ptr->listofv[j].size();//使用vector，没有value之间的分割符
                        }
                        last_offset = ptr->fk_size;//上一个节点的终止位置，作为下一次节点的seek起始位置，
                        base[i].fk_sum = base[i].fk_sum+1;//在循环里面多减掉了一个分割符号
                        vector<string> temp;
                        temp.swap(ptr->listofv);//将缓存清空！
                    }
                    ptr = ptr->next;
                }//至此，一个桶写入完成！
                out.flush();
            }
          ///下面这句话主要是为了检验程序是否正确，因此调完程序可以删除
          if(base[i].fk_sum != 0) cout<<"BUG:桶"<<i<<"中的缓存kv写入文件后，fk_sum还大于0！"<<endl;
        }//至此，文件sub_file涉及的所有桶中的kv对都写到了文件中。
    out.close();
    current_size_B = 0;//将计数器置为0
       /* map<string,string>::iterator it;
        it=gb->buffer.begin();
        string k =it->first;
        string v = it->second;/////////v是所有的value，下一步需要查找hash频率表，找到文件中偏移
       int bin_id = get_hash_id(k,59);
       int file_id =get_file_id(k,59);//获取文件id
       fstream out;
       out.open((basepath+"/"+inttostring(file_id)).c_str(),ios::out|ios::in);//以二进制形式
       if(!out){
           ofstream hehe;
           hehe.open((basepath+"/"+inttostring(file_id)).c_str());
           hehe.close();
        //cout<<"要写入分组文件："<<basepath+"/"+inttostring(file_id)<<"打不开！"<<endl;
        //exit(0);
           out.open((basepath+"/"+inttostring(file_id)).c_str(),ios::out|ios::in);//以二进制形式
           if(!out){
               cout<<"要写入分组文件111："<<basepath+"/"+inttostring(file_id)<<"打不开！"<<endl;
               exit(0);
           }
       }

        for(;it!=gb->buffer.end();){
            list_node * ptr = base[bin_id].next;//得到这个桶的第一个链表节点
            while(ptr!=NULL&&ptr->k.compare(k)!=0){
                ptr=ptr->next;//处理下一个节点
                /////每个链表节点代表一个key完事儿，也就是说一个key的处理完了，应该加个换行符号，是不？我觉得是，并且这些value之间也应该加个换行符号
              }
            if(ptr!=NULL&&ptr->k.compare(k)==0){//说明找到了
                /////该seek了！
                out.seekp(ptr->fk_size,ios::beg);//从文件的开始位置seek到fk_size个字节
                vector<string> split_v = splitByMyChar_returnVectorWithoutDimension(v,split_vv);
                for(int i=0;i<split_v.size();i++) {//把kv对写入文件
                    out << k << split_kv << split_v[i] <<split_vv;//形式就是key kv分隔符 value 换行
                    out.flush();
                    ptr->fk_size += k.size() + 1 + split_v[i].size() + 1;//追加了kvg
                }
            }
            else{cout<<"出错啦！有的kv‘对在hash频率表中找不到！"<<endl;
            }
            it++;
            if(it==gb->buffer.end()) break;
            k =it->first;
            v = it->second;/////////v是所有的value，下一步需要查找hash频率表，找到文件中偏移量
            bin_id = get_hash_id(k,59);
            if(get_file_id(k,59)!=file_id){
                 file_id = get_file_id(k,59);//不是上次那个文件id，则更新file_id,并且关闭上一个文件，打开新的文件
                out.close();
                out.open((basepath+"/"+inttostring(file_id)).c_str(),ios::in|ios::out);//以二进制形式
                if(!out){
                    ofstream hehe;
                    hehe.open((basepath+"/"+inttostring(file_id)).c_str());
                    hehe.close();
                    //cout<<"要写入分组文件："<<basepath+"/"+inttostring(file_id)<<"打不开！"<<endl;
                    //exit(0);
                    out.open((basepath+"/"+inttostring(file_id)).c_str(),ios::out|ios::in);//以二进制形式
                    if(!out){
                        cout<<"要写入分组文件2222："<<basepath+"/"+inttostring(file_id)<<"打不开！"<<endl;
                        exit(0);
                    }
                }

            }

        }
    out.close();//关闭最后一个打开的文件
    gb->buffer.clear();////释放map的所有内部节点
    gb->current_size_B=0;//清空buffer的同时，将计数器置为0*/
    return 0;

}
////////按顺序读取源文件，将kv对插入缓存buffer，查看缓存满否？满则将kv对写出，写出时根据hash频率统计，找到要插入的位置
int MyHash::key_grouping(char split_kv, char split_vv,string base_path, string outpath) {//在这里，将kv对不存储在GRroupingBuffer中，直接存在hash节点中
    for(int i=0;i<sub_file_num;i++){//显然这是一个文件一个文件的写出，默认缓存区只有一个文件中的kv对，也就是缓存设为小于一个文件的数据量
        ifstream in;//但是其实这并不合理对不？可能上一个文件缓存的kv对只有一点，下一个文件和它一起占用buffer_size，这时就要多个文件一起写入缓存，这里还没改写
        in.open((base_path+"/"+inttostring(i)).c_str(),ios::in);
        if(!in){
            cout<<base_path+"/"+inttostring(i)<<"打不开！"<<endl;
            exit(0);
        }
        string line;
        getline(in,line);
        while(!in.eof()){
         ////获取小文件中的一行kv存在line'中，根据split_kv分离出value值，插入对应缓存的map对儿
            string::size_type index = line.find_first_of(split_kv, 0);//找到空格、回车、换行等空白符
            string k = line.substr(0,index);
            string v= line.substr(index+1);

            //包含则找到key包含的value在后面追加上新的value
            if(full_or_not(v.size()+1)){//再加入就满了，则先把buffer中的写到文件里去
                flushtofile(outpath,split_kv,split_vv,i);//i是当前处理的文件号
                insert(k,v,split_vv);
            }//这样有一个问题，就是，当刚才插入的里面有6，但是被刷新到文件了，现在插入的只更新是不够的，
            else {//不满则直接插入
                insert(k,v,split_vv);
            }
            //////判断缓存满没，满则写出
            getline(in,line);
        }
        in.close();
        flushtofile(outpath,split_kv,split_vv,i);//最后将当前文件i中不满缓冲区的kv对写入文件！
    }
}
int MyHash::print_res_file(string outpath) {
    for(int i=0;i<sub_file_num;i++){
        ifstream in;
        in.open((outpath+"/"+inttostring(i)).c_str(),ios::in);
        string line;
        getline(in,line);
        cout<<"文件"<<outpath<<"/"<<i<<"****************************8"<<endl;
        while(!in.eof()){
            cout<<line<<endl;
            getline(in,line);
        }
        in.close();
    }
}
bool MyHash::full_or_not(uint64_t needToInsertKV_inB) {///满了为true
    if(current_size_B+needToInsertKV_inB>buffer_size_B) return true;
    else return false;
}

int MyHash::insert(string k, string v,char split_vv) {//将value插入到hash表的listofv部分，缓存下来！

    unsigned int seed = 59;//通常是一个质数，所有hash用同一个种子
    uint64_t bin_id  = get_hash_id(k,seed);
    //cout<<base+bin_id<<endl;

    if((base+bin_id)->next==NULL){//说明是第一个节点
        //  struct list_node  current_strut;    如果想保留退出本函数还存在的变量值，不要在栈上分配空间！，在堆上分配空间
        //struct list_node * current =& current_strut;
       cout<<"<"<<k<<","<<v<<">:插入listofv失败！"<<endl;
        // cout<<"current = "<<current<<endl;
    }else{
        list_node * pt =  base[bin_id].next,*prev = pt;
        while(pt!=NULL&&pt->k.compare(k)!=0){//pt的key小于k
            prev = pt;
            pt=pt->next;
        }//跳出循环的话说明走到链表尾部，说明pt的key大于等于当前key
        if(pt!=NULL&&(pt->k.compare(k)==0)){//pt的key等于k

            ///判断listofv 是否为null，是则listofv = v,不是 listofv += ,v
            if(pt->listofv.empty()){
                pt->listofv.push_back(v);
                current_size_B+=v.size();
                base[bin_id].fk_sum+=v.size();
            } else{//不是第一个value， listofv 加入v，
                pt->listofv.push_back(v);
                current_size_B+=v.size();
                base[bin_id].fk_sum+=v.size();
            }

        }
        else {//pt为空，说明当前key节点还没建立
            cout<<"<"<<k<<","<<v<<">:插入listofv失败！"<<endl;
        }
    }//// if((base+bin_id)->next==NULL){//说明是第一个节点
    return 0;
}
void MyHash::delete_hash_linkedlist() {
    for(int i=0;i<bin_num;i++){//遍历每一个桶
        list_node * current = base[i].next;
        while(current!=NULL){//当下一个不是空节点时
            list_node * next = current->next;
            delete  current;
            current = next;
        }
    }
}
MyHash::~MyHash() {
    ////循环释放链表及数组
    delete_hash_linkedlist();
    delete[] base;
}
