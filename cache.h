//
// Created by Kave Salamatian on 25/11/2018.
//

#ifndef BGPGEOPOLITICS_CACHE_H
#define BGPGEOPOLITICS_CACHE_H
#include <sqlite3.h>
#include <set>
#include <map>
#include <thread>
#include <boost/thread/shared_mutex.hpp>
#include "BGPGraph.h"
#include "BGPGeopolitics.h"
#include "BGPTables.h"
#include "BGPevent.h"
#include "tbb/concurrent_hash_map.h"
#include "BlockingQueue.h"
#include "apibgpview.h"
#include "BGPKafkaProducer.hpp"



using namespace std;
using namespace tbb;

//using namespace boost;

class Table;
class AS;
class Link;
class PrefixPath;
class Path;
class PrefixPeer;
class BGPMessage;
class EventTable;

using namespace boost;
using namespace std;


class BiStringMap {
public:
    map<string, int> leftMap;
    vector<string> rightMap;

    BiStringMap() {}
    string find(unsigned int id) const {
        boost::shared_lock<boost::shared_mutex> lock(mutex_);
        //shared_lock<shared_mutex> lock(mutex_);
        if (id < rightMap.size()) {
            return rightMap[id];
        }
        return NULL;
    }

    long find(string str) {
        map<string, int>::iterator it;

        boost::shared_lock<boost::shared_mutex> lock(mutex_);
        //shared_lock<shared_mutex> lock(mutex_);
        it = leftMap.find(str);
        if (it != leftMap.end()) {
            return it->second;
        } else {
            return -1;
        }
    }

    int insert(string str) {
        boost::unique_lock<boost::shared_mutex> lock(mutex_);
        int pos;
        auto it=leftMap.find(str);
        if (it == leftMap.end()){
            rightMap.push_back(str);
            pos= rightMap.size() - 1;
            leftMap.insert(pair<string, int>(str, rightMap.size() - 1));
        } else {
            pos = it->second;
        }
        return pos;
    }

private:
    mutable boost::shared_mutex mutex_;
};



class BGPCache {
public:

    map<unsigned int, AS *> asCache;
    concurrent_hash_map<string, Link *> linksMap;
    concurrent_hash_map<string, Path *> pathsMap;
    
    BiStringMap peerStrMap;
    BiStringMap peerPrefixStrMap;
    BiStringMap pathPrefixStrMap;
    BiStringMap pathStrMap;


    concurrent_hash_map<int,PrefixPath *> activesMap; // access by peer|prefix
    concurrent_hash_map<int,PrefixPath *> inactivesMap; // access by pathstr|prefix

    map<string, PrefixPeer *> prefixPeerMap;
    EventTable *eventTable;
    std::thread apiThread;
    BlockingCollection<AS *> toAPIbgpbiew;
    BGPGraph *bgpg;
    BGPCache(string dbname, BGPGraph *g, EventTable *eventTable): bgpg(g), eventTable(eventTable) {
        if (sqlite3_open(dbname.c_str(), &db) != SQLITE_OK) {
            cout << "Can't open database: " << sqlite3_errmsg(db) << endl;
        }
        apibgpview = new APIbgpview(db, toAPIbgpbiew, this);
        apiThread = std::thread(&APIbgpview::run, apibgpview);

    }

    int fillASCache();
    AS *updateAS(int asn, AS *as);
    AS *chkAS(unsigned int asn, unsigned int time);
    //modified by zxy
    //void updateBGPentry(PrefixPath* prefixPath, PrefixPeer * prefixPeer, unsigned int time);
    //void setPathActive(PrefixPath *prefixPath, PrefixPeer * prefixPeer, unsigned int time);
    //void setPathNonActive(PrefixPath *prefixPath, PrefixPeer *prefixPeer, unsigned int time);
    //void removePrefixPeer(PrefixPeer *prefixPeer, unsigned peer, unsigned int asn,unsigned int time);
    void setPathActive(PrefixPath *prefixPath, PrefixPeer * prefixPeer, unsigned int time, ProducerKafka * producer);
    void setPathNonActive(PrefixPath *prefixPath, PrefixPeer *prefixPeer, unsigned int time, ProducerKafka * producer);
    void updateBGPentry(PrefixPath* prefixPath, PrefixPeer * prefixPeer, unsigned int time, ProducerKafka * producer);
    void removePrefixPeer(PrefixPeer *prefixPeer, unsigned peer, unsigned int asn,unsigned int time, ProducerKafka *producer);
    //end of modify
    long size_of();
private:
    sqlite3 *db;
    APIbgpview *apibgpview;
};




#endif //BGPGEOPOLITICS_CACHE_H
