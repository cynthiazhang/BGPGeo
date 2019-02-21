#include <iostream>
#include <thread>
#include <list>
#include "BlockingQueue.h"
#include "BGPGeopolitics.h"
#include "BGPSource.h"
#include "cache.h"
#include "BGPGraph.h"
#include "BGPTables.h"
#include "BGPSaver.h"
#include "BGPKafkaProducer.hpp"

class Wrapper {
    std::thread source, table1, table2, table3, table4, save;
public:
    Wrapper(int t_start, int t_end, int dumpDuration, std::list<std::string>& collectors ,  std::string& captype, int version) {
        PriorityBlockingCollection<BGPMessage *,  PriorityContainer<BGPMessage *, BGPMessageComparer>> toTableFlag(20000);
        BlockingCollection<BGPMessage *> toSaver(20000);
        BGPGraph g;
        EventTable eventTable;
        BGPCache cache("resources/as.sqlite", &g, &eventTable);
        BGPMessagePool bgpMessagePool(&cache,10000);
        BGPTables *bgpTables = new BGPTables(&cache, version);
        
        //Added by zxy
        //init kafka
        ProducerKafka* producer = new ProducerKafka;
        if (PRODUCER_INIT_SUCCESS == producer->init_kafka(0, "192.168.1.108:9092", "zhangxinyi")){
            cout<<"Kafka producer init success"<<endl;
        }
        else{
            cout<<"Kafka producer init faile"<<endl;
        }
        //end of add
        
        cache.fillASCache();
        BGPSource *bgpsource = new BGPSource(&bgpMessagePool, toTableFlag,  t_start, t_end, collectors ,  captype, 4, &cache);
        
        //modified by zxy
        //TableFlagger *tableFlagger = new TableFlagger(toTableFlag, toSaver, bgpTables, &cache, bgpsource, 4);
        TableFlagger *tableFlagger = new TableFlagger(toTableFlag, toSaver, bgpTables, &cache, bgpsource, 4, producer);
        //end of modify
        ScheduleSaver *saver = new ScheduleSaver(&g, t_start, dumpDuration, toSaver, &cache, bgpsource);
        source = std::thread(&BGPSource::run, bgpsource);
        table1 = std::thread(&TableFlagger::run, tableFlagger);
        table2 = std::thread(&TableFlagger::run, tableFlagger);
        table3 = std::thread(&TableFlagger::run, tableFlagger);
        table4 = std::thread(&TableFlagger::run, tableFlagger);
        save = std::thread(&ScheduleSaver::run, saver);
        table1.join();
        table2.join();
        table3.join();
        table4.join();
        source.join();
        
    }
    
};


int main(int argc, char **argv) {
    int start = 1545994220-700*24*60*60;
    int end = start+24*60*60;

    std::string mode ="WR";
    int dumpDuration =60;
    
    std::list<std::string> collectors;
    collectors.push_back("rrc00");
    collectors.push_back("rrc01");
    collectors.push_back("rrc02");
    collectors.push_back("rrc03");
    collectors.push_back("rrc04");
    collectors.push_back("rrc05");
    collectors.push_back("rrc06");
    collectors.push_back("rrc09");
    collectors.push_back("rrc10");
    collectors.push_back("rrc11");
    collectors.push_back("rrc12");
    collectors.push_back("rrc13");
    collectors.push_back("rrc14");
    collectors.push_back("rrc15");
    collectors.push_back("rrc16");
    collectors.push_back("rrc17");
    collectors.push_back("rrc18");
    collectors.push_back("rrc19");
    collectors.push_back("rrc20");
    collectors.push_back("rrc21");
    Wrapper *w = new Wrapper(start, end, dumpDuration, collectors, mode,4);
    return 0;
}



