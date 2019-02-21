//
// Created by Kave Salamatian on 2018-12-01.
//

#ifndef BGPGEOPOLITICS_BGPTABLES_H
#define BGPGEOPOLITICS_BGPTABLES_H

#include "cache.h"
#include "BGPSource.h"
#include "BGPGeopolitics.h"
#include "BlockingQueue.h"
#include "BGPevent.h"
#include <map>
#include "BGPKafkaProducer.hpp"



class PrefixPath;
class PrefixPeer;
class EventTable;
class BGPSource;

class PrefixPathComparer {
public:
    PrefixPathComparer() {}
    int operator() (const PrefixPath* prefixPath, const PrefixPath* new_prefixPath);
};



class PrefixElement {
public:
    PrefixPath *bestPath= NULL;
    PriorityBlockingCollection<PrefixPath *,  PriorityContainer<PrefixPath *, PrefixPathComparer>> orderPaths;

    PrefixElement(PrefixPeer *prefixPeer) : prefixPeer(prefixPeer) {};
    void addPath(PrefixPath * prefixPath, unsigned int time);
    bool setBestPath();
private:
    mutable boost::shared_mutex mutex_;
    boost::unique_lock<boost::shared_mutex> uniquemutex_;

    PrefixPeer *prefixPeer;
};

class CollectorElement{
public:
    std::string collector;
    CollectorElement(std::string collector): collector(collector){}
    concurrent_hash_map<std::string, PrefixElement*> prefixElements;
    void addPath(PrefixPath *prefixPath, PrefixPeer * prefixPeer, unsigned int time);
private:
};

class BGPMessage;
class BGPCache;

class Table{
public:
    concurrent_hash_map<std::string, CollectorElement*> collectorElements;
    Table(unsigned int version, BGPCache *cache): version(version), cache(cache){}
    
    //modified by zxy
    //bool update(BGPMessage *bgpMessage);
    bool update(BGPMessage *bgpMessage,ProducerKafka* producer);
    //end of modify
    bool updateBestPath(BGPMessage *bgpMessage, PrefixPath *active, PrefixPath *newpath, bool withdraw);
private:
    unsigned int version;
    BGPCache *cache;

};

class PrefixElem;
class BGPTables{
public:
    Table *tableV4;
    Table *tableV6;
    BGPCache *cache;
    int version;

    BGPTables(BGPCache *cache, int version):  cache(cache), version(version){
        tableV4 = new Table(4,cache);
        tableV6 = new Table(6,cache);
    }
    //modified by zxy
    //BGPMessage *update(BGPMessage *bgpMessage);
    BGPMessage *update(BGPMessage *bgpMessage,ProducerKafka* producer);
    //end of modify
};

class BGPMessageComparer;
class TableFlagger{
public:
    //modified by zxy
    //TableFlagger(PriorityBlockingCollection<BGPMessage *,  PriorityContainer<BGPMessage *, BGPMessageComparer>>
    //       &infifo, BlockingCollection<BGPMessage *> &outfifo, BGPTables *bgpTables, BGPCache *cache, BGPSource *bgpSource, int version);
    TableFlagger(PriorityBlockingCollection<BGPMessage *,  PriorityContainer<BGPMessage *, BGPMessageComparer>>
          &infifo, BlockingCollection<BGPMessage *> &outfifo, BGPTables *bgpTables, BGPCache *cache, BGPSource *bgpSource, int version, ProducerKafka* producer);
    //end of modify
    
    void run();
private:
    BlockingCollection<BGPMessage *> &outfifo;
    PriorityBlockingCollection<BGPMessage *,  PriorityContainer<BGPMessage *, BGPMessageComparer>> &infifo;
    BGPCache *cache;
    BGPTables *bgpTables;
    BGPSource *bgpSource;
    int version;
    //added by zxy
    ProducerKafka* producer;
    //end of add
};




#endif //BGPGEOPOLITICS_BGPTABLES_H
