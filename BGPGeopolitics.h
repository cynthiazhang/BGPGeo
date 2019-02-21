//
// Created by Kave Salamatian on 24/11/2018.
//

#ifndef BGPGEOPOLITICS_BGPGEOPOLITICS_H
#define BGPGEOPOLITICS_BGPGEOPOLITICS_H
#include "stdint.h"
extern "C" {
#include "bgpstream.h"
}
#include "cache.h"
#include "tbb/concurrent_unordered_set.h"
#include <set>
#include <mutex>
#include <condition_variable>
#include <list>
#include <vector>

using namespace std;

class BGPCache;
class PeerPrefix;
class PrefixPath;
class Path;


enum Category{None, AADiff,AADup, WADup, WWDup, Flap, Withdrawn};


enum Status{CONNECTED=1, DISCONNECTED=2, OUTAGE=4, HIJACKED=8, HIJACKING=16};



template < class T> class ThreadSafeSet{
public:
    std::set<T> set;
    bool insert(const T& val){
        boost::unique_lock<boost::shared_mutex> lock(mutex_);
        return set.insert(val).second;
    }

    int erase(const T& val){
        boost::unique_lock<boost::shared_mutex> lock(mutex_);
        return set.erase(val);
    }

    int size(){
        boost::shared_lock<boost::shared_mutex> lock(mutex_);
        return set.size();
    }

    typename std::set<T>::iterator find(const T& val){
        boost::shared_lock<boost::shared_mutex> lock(mutex_);
        return set.find(val);
    }

    typename std::set<T>::iterator end(){
        boost::shared_lock<boost::shared_mutex> lock(mutex_);
        return set.end();
    }

    typename std::set<T>::iterator begin(){
        return set.begin();
    }


private:
    mutable boost::shared_mutex mutex_;
};

class PrefixPeer{
public:
//    string str;
    bgpstream_pfx_storage_t *pfx;
    ThreadSafeSet<short int> peerSet;
    ThreadSafeSet<unsigned int> asSet;

    PrefixPeer(bgpstream_pfx_storage_t *prefix, unsigned int time): cTime(time){
        pfx = new bgpstream_pfx_storage_t();
        memcpy(pfx, prefix, sizeof(bgpstream_pfx_storage_t));
    }
    int addPeer(short int peerStrIndex, unsigned int time){
        peerSet.insert(peerStrIndex);
        cTime=time;
        return peerSet.size();
    }

    int addAS(unsigned int asn){
        asSet.insert(asn);
        return asSet.size();
    }

    int removePeer(short int peerStrIndex, unsigned int time){
        auto it = peerSet.find(peerStrIndex);
        if (it != peerSet.end()){
            peerSet.erase(peerStrIndex);
            cTime = time;
        }
        return peerSet.size();
    }

    int removeAS(unsigned int asn){
        if (asSet.find(asn) != asSet.end()){
            asSet.erase(asn);
        }
        return asSet.size();
    }

    bgpstream_addr_version_t getVersion(){
        return pfx->address.version;
    }

    bool checkHijack(){
        if (asSet.size()>2) {
            return true;
        }
        return false;
    }

    int size_of(){
        int size =0;
        string str;
        for(auto it=peerSet.begin();it!=peerSet.end();it++){
            size +=2;
        }
        size += asSet.size()*4;
        return size;
    }

    string str(){
        char buf[20];
        bgpstream_pfx_snprintf(buf, 20, (bgpstream_pfx_t *)pfx);
        string str(buf);
        return str;
    }

private:
    unsigned int cTime;
};

class BGPMessage{
public:
    int poolOrder;
    long messageOrder;
    string collector;
    bgpstream_elem_type_t  type;
    uint32_t  timestamp;
    bgpstream_addr_storage_t  peerAddress;
    uint32_t  peerASNumber;
    bgpstream_addr_storage_t  nextHop;
    PrefixPeer  *prefixPeer;
    vector<unsigned int> asPath;
    vector<unsigned int> shortPath;
    BGPCache *cache;
    PrefixPath *prefixPath=NULL, *activePrefixPath=NULL, *previousPrefixPath=NULL;
    bool newPath = false;
    string pathString="[";
    Category category = None;


//    BGPMessage(bgpstream_elem_t *elem, string collector, BGPCache *cache);
    BGPMessage(BGPCache *cache, int order);
    void fill(long order, bgpstream_elem_t *elem, unsigned int time, string collector, BGPCache *cache);
    void preparePath();
    double fusionRisks(double geoRisk, double secuRisk, double otherRisk);
    void shortenPath();
};

class BGPMessageComparer {
public:
    BGPMessageComparer() {}

    int operator() (const BGPMessage* bgpMessage, const BGPMessage* new_bgpMessage) {
        if (bgpMessage->messageOrder < new_bgpMessage->messageOrder)
            return -1;
        else if (bgpMessage->messageOrder > new_bgpMessage->messageOrder)
            return 1;
        else
            return 0;
    }
};



class AS{
public:
    unsigned int asNum;
    string name="??";
    string country="??";
    string RIR="??";
    double risk=0.0, geoRisk=0.0, secuRisk=0.0,  otherRisk=0.0;
    unsigned int activePrefixNum=0;
    unsigned int allPrefixNum=0;
    concurrent_hash_map<string, PrefixPeer *> activePrefixMap;
    ThreadSafeSet<PrefixPeer *> inactivePrefixSet;
    unsigned int cTime=0;
    bool observed=false;
    int status=0;

    AS(int asn): asNum(asn){}
    AS(int asn, string inname, string incountry, string(inRIR), float risk): asNum(asn){
        name = inname;
        country = incountry;
        RIR = RIR;
    }
    bool checkOutage(){
        activePrefixNum = activePrefixMap.size() ;
        allPrefixNum =  activePrefixNum+ inactivePrefixSet.size();
        if ((activePrefixNum<0.3*allPrefixNum) &&(allPrefixNum>10))  {
            status |= OUTAGE;
            return true;
        }   else {
            if (status & OUTAGE) {
                status &= ~OUTAGE;
            }
            return false;
        }
    }

    void disconnect(){
        status |= DISCONNECTED;
    }

    void reconnect(){
        status &= ~DISCONNECTED;
    }

    int size_of(){
        int size =4+4*8+4*4+1;
        size += name.length();
        size += country.length();
        size += RIR.length();
        for(auto it=activePrefixMap.begin();it!=activePrefixMap.end();it++)
            size += it->first.length()+4;
        return size;
    }
};

class Link{
public:
    unsigned int src;
    unsigned int dst;
    unsigned int cTime;
    Status status;
    concurrent_hash_map<string, PrefixPeer *> activePrefixMap;
    Link(unsigned int src, unsigned int dst, unsigned int time): src(src), dst(dst), cTime(time){}
};

class PrefixPath{
public:
    Path *path;
    PrefixPeer* prefixPeer;
    string collector;
    bool active= true;
    unsigned int lastChange = 0;
    unsigned int lastActive = 0;
    int announcementNum =0;
    int AADiff =0, AADup=0, WADup=0, WWDup=0, Flap=0, Withdraw=0;
    float coeff = 0.9;
    double globalRisk=0.0;
    double meanUp, meanDown;

    PrefixPath(string collector, Path *path, PrefixPeer *prefixPeer): collector(collector), path(path), prefixPeer(prefixPeer){}
    double getScore() const;
    int size_of();
};

class Path{
public:
//    vector<unsigned int> asPath;
    int pathLength;
    vector<unsigned int> shortPath;
//    vector<string> geoPath;
//    vector<string> asNames;
    concurrent_unordered_set<PrefixPeer *> prefixPeerSet;
//    string pathString;
    double risk = 0;
/*    Path(BGPMessage* bgpMessage): asPath(bgpMessage->asPath), shortPath(bgpMessage->shortPath), pathString(bgpMessage->pathString) {}
 */
    Path(BGPMessage* bgpMessage): shortPath(bgpMessage->shortPath), pathLength(bgpMessage->asPath.size()){}
    string pathString(){
        string pathString ="[";
        for (auto asn:shortPath) {
            pathString += to_string(asn)+",";
        }
        pathString = pathString+"]";
        return pathString;
    }
    
    int size_of();
};





#endif //BGPGEOPOLITICS_BGPGEOPOLITICS_H
