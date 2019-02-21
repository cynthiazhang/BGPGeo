//
//  BGPSource.cpp
//  BGPGeo
//
//  Created by Kave Salamatian on 18/01/2019.
//  Copyright © 2019 Kave Salamatian. All rights reserved.
//

#include <stdio.h>
#include "BGPSource.h"

BGPMessagePool::BGPMessagePool(BGPCache *cache, int capacity): capacity(capacity), bgpMessages(capacity) {
    for(int i=0;i<capacity;i++){
        bgpMessages.add(new BGPMessage(cache, i));
    }
}

BGPMessage* BGPMessagePool::getBGPMessage(int order, bgpstream_elem_t *elem, unsigned int time, std::string collector, BGPCache* cache){
    BGPMessage *bgpMessage;
    bgpMessages.take(bgpMessage);
    bgpMessage->fill(order, elem, time, collector, cache);
    return bgpMessage;
}

void BGPMessagePool::returnBGPMessage(BGPMessage* bgpMessage) {
    bgpMessages.add(bgpMessage);
}

void BGPSource::returnBGPMessage(BGPMessage* bgpMessage){
    BGPMessage* bgpMessage1;
    string peerStr = bgpMessage->collector + '|' + to_string(bgpMessage->peerASNumber);
    string str = bgpMessage->prefixPeer->str();
    string peerPrefixStr= peerStr + '|' + str;
    BlockingCollection<BGPMessage *> *queue;
    //modified by xinyi ZHANG
    //queue = (BlockingCollection<BGPMessage *> *) malloc (sizeof (BlockingCollection<BGPMessage *>));
    //end of add
    concurrent_hash_map<string, BlockingCollection<BGPMessage *> *>::accessor acc;
    
    
    if (inProcess.find(acc, peerPrefixStr)){
        queue= acc->second;
        if (!queue->is_empty()){
            queue->take(bgpMessage1);
            acc.release();
            bgpMessage1->preparePath();
            fifoQueue.add(bgpMessage1);
        } else {
            acc.release();
            inProcess.erase(peerPrefixStr);
            //modified by xinyi ZHANG
            //free(queue);
            //end of modified
        }
    }
    bgpMessagePool->returnBGPMessage(bgpMessage);
}


bool BGPMessagePool::isPoolAvailable(){
    return !bgpMessages.is_empty();
}



BGPSource::BGPSource(BGPMessagePool *bgpMessagePool,PriorityBlockingCollection<BGPMessage *,  PriorityContainer<BGPMessage *, BGPMessageComparer>> &fifo,
                     int t_start, int t_end, std::list<std::string> &collectors, std::string &captype, int version, BGPCache *cache) :
bgpMessagePool(bgpMessagePool), fifoQueue(fifo), t_start(t_start), t_end(t_end), version(version), cache(cache) {
    /* Set metadata filters */
    bs = bgpstream_create();
//    record = bgpstream_record_create();
    for (auto const &collector : collectors) {
        bgpstream_add_filter(bs, BGPSTREAM_FILTER_TYPE_COLLECTOR, collector.c_str());
    }
    if (captype == "BR") { //begin with RIBs
        bgpstream_add_filter(bs, BGPSTREAM_FILTER_TYPE_RECORD_TYPE, "ribs");
        mode = 0;
    } else if (captype == "OR") { //only RIBs
        bgpstream_add_filter(bs, BGPSTREAM_FILTER_TYPE_RECORD_TYPE, "ribs");
        mode = 0;
    } else { //without RIBs
        bgpstream_add_filter(bs, BGPSTREAM_FILTER_TYPE_RECORD_TYPE, "updates");
        mode = 1;
    }
    /* Time interval: t_start, t_end */
    bgpstream_add_interval_filter(bs, t_start, t_end);
    
    /* Start the stream */
}

int BGPSource::run() {
    bgpstream_elem_t *elem;
    BGPMessage *bgpMessage;   
    char buffer[1024]; 
    string peerStr;
    string str;
    string peerPrefixStr; 
    unsigned long order=0;
    std::string collector;
    unsigned int time;
    
    concurrent_hash_map<string, BlockingCollection<BGPMessage *> *>::accessor acc;
    
    
    bgpstream_start(bs);
    /* Read the stream of records */
    while (bgpstream_get_next_record(bs, &record) > 0) {
        /* Ignore invalid records */
        if (record->status != BGPSTREAM_RECORD_STATUS_VALID_RECORD) {
            continue;
        }
//        std::string collector(record->attributes.dump_collector);
        collector = string{record->collector_name};
        time = record->time_sec;
        /* Extract elems from the current record */
//        while ((elem = bgpstream_record_get_next_elem(record)) != NULL) {
        while (bgpstream_record_get_next_elem(record, &elem)>0) {
            if (elem->type == BGPSTREAM_ELEM_TYPE_ANNOUNCEMENT ||
                elem->type == BGPSTREAM_ELEM_TYPE_WITHDRAWAL ||
                elem->type == BGPSTREAM_ELEM_TYPE_RIB) {
                order ++;
                bool proceed = false;
                if (elem->prefix.address.version == BGPSTREAM_ADDR_VERSION_IPV6) {
                    if ((version==6) || (version==64)){
                        proceed=true;
                    }
                } else {
                    if ((version==4) || (version==64)){
                        proceed=true;
                    }
                }
                if (proceed) {
                    bgpMessage = bgpMessagePool->getBGPMessage(order, elem, time, collector, cache);
                    peerStr = bgpMessage->collector + '|' + to_string(bgpMessage->peerASNumber);
                    str = bgpMessage->prefixPeer->str();
                    peerPrefixStr= peerStr + '|' + str;
                    if (inProcess.find(acc, peerPrefixStr)){
                        acc->second->add(bgpMessage);
                        acc.release();
                    } else{
                        acc.release();
                        inProcess.insert(pair<string, BlockingCollection<BGPMessage *> *>(peerPrefixStr, new BlockingCollection<BGPMessage *>()) );
                        bgpMessage->preparePath();
                        fifoQueue.add(bgpMessage);
                    }
                }
            }
            count++;
            //                std::cout << bgpMessagePool->bgpMessages.size()<<std::endl;
            if (count % 100000 == 0) {
                std::cout<<count<<std::endl;
            }
        }
//        bgpstream_record_clear(record);
    }
    std::cout << "FINISH" << std::endl;
    bgpstream_destroy(bs);
    return 0;
}

