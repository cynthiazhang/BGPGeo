//
// Created by Kave Salamatian on 2018-12-05.
//

#ifndef BGPGEOPOLITICS_BGPSAVER_H
#define BGPGEOPOLITICS_BGPSAVER_H

#include <boost/filesystem.hpp>
#include "json.hpp"
#include <iostream>
#include <fstream>
#include <chrono>

using namespace std;
using namespace boost::filesystem;
using json = nlohmann::json;
using namespace std::chrono;

class Stats {
public:
    int numBGPmsgAll = 0, numBGPlastsec = 0, numUpdates = 0, numWithdraw = 0, numRIB = 0, numPathall = 0, numNewPathlastSec = 0,
        numPrefixall = 0, numPrefixlastsec = 0, numCollector = 0, streamQueuesize = 0, details = 0, numActivepaths = 0,
        numNewactivepaths = 0, numAS =0, numLink = 0, processTime =0, numInactivePath=0;
    unsigned int time;
    double delay = 0.0;
    BGPCache *cache= NULL;
    high_resolution_clock::time_point start=high_resolution_clock::now(),end;
    Stats(unsigned int time): time(time) {}
    Stats(BGPCache *bgpcache): cache(cache){
    }
    void fill(Stats& stats){
        time = stats.time;
        numBGPmsgAll =  stats.numBGPmsgAll;
        numBGPlastsec = stats.numBGPlastsec;
        numUpdates = stats.numUpdates;
        numWithdraw = stats.numWithdraw;
        numRIB = stats.numRIB;
        numPathall = stats.numPathall;
        numNewPathlastSec = stats.numNewPathlastSec;
        numPrefixall = stats.numPrefixall,
        numPrefixlastsec = stats.numPrefixlastsec;
        numCollector = stats.numCollector;
        streamQueuesize = stats.streamQueuesize;
        details = stats.details;
        numActivepaths= stats.numActivepaths;
        numNewactivepaths = stats.numNewactivepaths;
        processTime = stats.processTime;
        numInactivePath = stats.numInactivePath;
    }

    void update(BGPMessage* bgpMessage){
        numBGPmsgAll +=1;
        switch (bgpMessage->type ){
            case BGPSTREAM_ELEM_TYPE_ANNOUNCEMENT:
                numUpdates +=1;
                break;
            case BGPSTREAM_ELEM_TYPE_WITHDRAWAL:
                numWithdraw +=1;
                break;
            case BGPSTREAM_ELEM_TYPE_RIB:
                numRIB +=1;
                break;
            default:
                break;
        }
    }

    void makeReport(Stats laststats, unsigned int inTime){
        duration<double, std::milli> processDuration;
        time= inTime;
        numPathall= cache->pathsMap.size();
        numActivepaths = cache->activesMap.size();
        numInactivePath = cache->inactivesMap.size();
        numAS= num_vertices(cache->bgpg->g);
        numLink = num_edges(cache->bgpg->g);
        numPrefixall = cache->prefixPeerMap.size();
        numBGPlastsec = numBGPmsgAll - laststats.numBGPmsgAll;
        numNewPathlastSec = numPathall - laststats.numPathall;
        numPrefixlastsec = numPrefixall - laststats.numPrefixall;
        numNewactivepaths = numActivepaths -  laststats.numActivepaths;
        end = high_resolution_clock::now();
        processDuration=(end-start);
        processTime= processDuration.count();
        delay=processTime*1.0 /numBGPlastsec*1000; //in usec
        start= end;
    }

    string toJson(string &str){
        json j,j1;
        j["processDelay"] = delay;
        j["processTime"] = processTime;
        j["numBGPmsgAll"] = numBGPmsgAll;
        j["numBGPlastsec"]=numBGPlastsec;
        j["numUpdates"]=numUpdates;
        j["numWithdraw"]=numWithdraw;
        j["numRIB"]=numRIB;
        j["numPathall"]=numPathall;
//        j["numNewPathlastSec"]=numNewPathlastSec;
        j["numPrefixall"]=numPrefixall;
//        j["numPrefixlastsec"]=numPrefixlastsec;
        j["numCollector"]=numCollector;
        j["numActivepaths"]=numActivepaths;
        j["numInactivepaths"]= numInactivePath;
//        j["numNewactivepaths"]=numNewactivepaths;
        j["numAS"]=numAS;
        j["numLink"]=numLink;
        j1[to_string(time)]=j;
        str = j1.dump();
        return str;
    }

    void printStr(){
        string str;
        cout<<toJson(str)<<endl;
    }
};

class ScheduleSaver{
public:
    ScheduleSaver( BGPGraph *bgpg, int start, int dumpDuration, BlockingCollection<BGPMessage *> &infifo,
            BGPCache *bgpcache, BGPSource *bgpSource):  bgpg(bgpg), time(start), dumpDuration(dumpDuration),
            infifo(infifo), cache(bgpcache), bgpSource(bgpSource), lastStats(start), stats(start){

        string dumpath="/Users/zhangxinyi/dumps";
        path p=path(dumpath);
        stats.cache = cache;
        lastStats.cache = cache;
        if (!exists(p) || !is_directory(p)) {
            create_directory(p);
        }
        perfFileName = dumpath+"/perf"+to_string(time)+".dat";
        eventsFileName = dumpath+"/events"+to_string(time)+".dat";
    }

    void saveGraph(){
        bgpg->save(dumpath+"/graphdumps"+to_string(time)+"."+to_string(time+dumpDuration)+".graphml");
    }

    void saveEvents(){
        unsigned int asn;
        Events *events;
        Event *p;
        string str="{[";
        eventFile.open(eventsFileName, ios::trunc);
        json j, j1, j2, j3, j4;
        map<unsigned int, Events *> eventASMap= cache->eventTable->eventASMap;
        for(auto it=eventASMap.begin(); it!=eventASMap.end(); it++){
            j.clear();
            j1.clear();
            j2.clear();
            j3.clear();
            events = it->second;
            for (auto it1=events->activeEvents.begin();it1 != events->activeEvents.end(); it1++){
                j2.clear();
                p = *it1;
                switch (p->type){
                    case OUTAGE_EV:
                        if (time-p->bTime>300){
                            j2 = json{{"type", p->type},{"bTime", p->bTime},
                                      {"outpfxnum",p->involvedPrefixNum},{"allpfxnum",p->allPrefixNum}};
                        }
                        break;
                    case HIJACK_EV:
                        if (time-p->bTime>300){

                            j2 = json{{"type", p->type},{"hijacking", p->asn},{"bTime", p->bTime},
                                      {"invpfxnum",p->involvedPrefixNum},{"pfx",p->allPrefixNum}};
                            j4.clear();
                            for (auto it2=p->involvedPrefixSet.begin();it2!=p->involvedPrefixSet.end(); j4.push_back((*it2++)->str()));
                            j2["pfxs"]=j4;
                        }
                        break;
                }
                if (!j2.empty()){
                    str =j2.dump();
                    j3.push_back(j2);
                }
            }
            if (!j3.empty()){
                j1["active"]=j3;
                j3.clear();
            }
            j2.clear();
            j3.clear();
            for (auto it1=events->inactiveEvents.begin();it1 != events->inactiveEvents.end(); it1++){
                p = *it1;
                j2.clear();
                j2.clear();
                p = *it1;
                switch (p->type){
                    case OUTAGE_EV:
                        if (time-p->bTime>300){
                            j2 = json{{"type", p->type}, {"bTime", p->bTime}, {"eTime", p->eTime} ,
                                      {"outpfxnum",p->involvedPrefixNum},{"allpfxnum",p->allPrefixNum}};
                        }
                        break;
                    case HIJACK_EV:
                        if (time-p->bTime>300){

                            j2 = json{{"type", p->type}, {"hijacking", p->asn},{"bTime", p->bTime}, {"eTime", p->bTime},
                                      {"invpfxnum",p->involvedPrefixNum},{"pfx",p->allPrefixNum}};
                            j4.clear();
                            for (auto it2=p->involvedPrefixSet.begin();it2!=p->involvedPrefixSet.end(); j4.push_back((*it2++)->str()));
                            j2["pfxs"]=j4;
                        }
                        break;
                }
                if (!j2.empty()){
                    j3.push_back(j2);
                    str= j2.dump();
                }
            }
            if (!j2.empty()) {
                str= j2.dump();
                j1["inactive"] = j3;
                j3.clear();
            }
            if (!j1.empty()){
                j[to_string(events->asNum)]=j1;
                str = j.dump();
                eventFile<<j.dump()<<endl;
            }
        }
        eventFile.close();
    }


    void run(){
        BGPMessage *bgpmessage;
        string str;
        bool cont= true;
        previoustime =0;
        perfFile.open(perfFileName);
        perfFile<<"{";
        while (cont){
           if (infifo.try_take(bgpmessage, std::chrono::milliseconds(120000))==BlockingCollectionStatus::TimedOut){
//               previoustime = bgpmessage->timestamp;
               cont = false;
               cout<< "Data Famine Saver"<<endl;
               saveGraph();
               cout<<"save !!!!!!!!!!!!!!!!!!!!" + to_string(time) + " to " + to_string(time + dumpDuration)<<endl;
               break;
            } else {
                stats.update(bgpmessage);
            }
            if (bgpmessage->timestamp>time+dumpDuration-1){
                previoustime=bgpmessage->timestamp;
                stats.makeReport(lastStats, previoustime);
                perfFile<<stats.toJson(str)<<","<<endl;
                lastStats.fill(stats);
                saveGraph();
//                saveEvents();
                cout<<"save !!!!!!!!!!!!!!!!!!!!" + to_string(time) + " to " + to_string(time + dumpDuration)<<endl;
 //               cout<<"Cache Size:"<<cache->size_of()<<endl;
                time +=dumpDuration;
            }
            count++;
//            if (count%10000 ==0){
//                cout<<"Stats:";
//                stats.printStr();
//            }
        }
        perfFile<<"}"<<endl;
        perfFile.close();
        cout<<"Processing Done!"<<endl;
    }

private:
    BGPGraph *bgpg;
    int time, dumpDuration;
    BlockingCollection<BGPMessage *> &infifo;
    Stats stats;
    Stats lastStats;
    std::string dumpath ="/Users/zhangxinyi/Dumps";
    unsigned int previoustime;
    BGPCache *cache;
    int count =0;
    BGPSource *bgpSource;
    string perfFileName;
    string eventsFileName;
    std::ofstream perfFile;
    std::ofstream eventFile;
};
#endif //BGPGEOPOLITICS_BGPSAVER_H
