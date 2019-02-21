//
// Created by Kave Salamatian on 2018-12-03.
//
#include "BGPTables.h"
#include "BGPGeopolitics.h"

using namespace std; 

int PrefixPathComparer::operator() (const PrefixPath* prefixPath, const PrefixPath* new_prefixPath) {
    if (prefixPath->getScore() < new_prefixPath->getScore())
        return -1;
    else if (prefixPath->getScore() > new_prefixPath->getScore())
        return 1;
    else
        return 0;
}


void PrefixElement::addPath(PrefixPath * prefixPath, unsigned int time){
    orderPaths.add(prefixPath);
}


bool PrefixElement::setBestPath() {
    boost::unique_lock<boost::shared_mutex> lock(mutex_);
    bool collectorOutage =true;
    bestPath = NULL;
    while (orderPaths.size() > 0) {
        //There are other paths so no Outage at collector level
        orderPaths.take(bestPath);
        if (bestPath->active) {
            collectorOutage =false;
            orderPaths.add(bestPath);
            break;
        } else {
            bestPath = NULL;
        }
    }
    return collectorOutage;
}

bool Table::update(BGPMessage *bgpMessage,ProducerKafka* producer){
    concurrent_hash_map<std::string, PrefixElement*>::iterator it;
    PrefixElement *prefixElement;
    PrefixPath *prefixPath= bgpMessage->prefixPath, *active=bgpMessage->activePrefixPath, *previous = bgpMessage->previousPrefixPath,
        *newPath;
    concurrent_hash_map<std::string, CollectorElement*>::const_accessor acc;
    concurrent_hash_map<std::string, PrefixElement*>::const_accessor acc1;

    unsigned int time = bgpMessage->timestamp;
    bool withdraw = false, collectorOutage = false;

    if ((bgpMessage->type == BGPSTREAM_ELEM_TYPE_ANNOUNCEMENT) || (bgpMessage->type == BGPSTREAM_ELEM_TYPE_RIB)){
        prefixPath->announcementNum++;
        if (bgpMessage->shortPath.size()>0){
            //find prefixElement in Collector Routing table
            if (collectorElements.find(acc, bgpMessage->collector)){
                if (!acc->second->prefixElements.find(acc1, bgpMessage->prefixPeer->str())) {
                    prefixElement = new PrefixElement(bgpMessage->prefixPeer);
                    acc->second->prefixElements.insert(pair<string,PrefixElement *>(bgpMessage->prefixPeer->str(), prefixElement));
                }
            }
            if (bgpMessage->newPath) {
                // this path has never been announced, this is a new one
                if (active != NULL){
                    //This is an implicit withdraw
                    withdraw = true;
                    bgpMessage->category = AADiff; // implicit withdrawal and replacement with different
                    active->AADiff++;
                    active->meanUp = active->coeff*active->meanUp+
                                     (1-active->coeff)*(time-active->lastActive);
                    active->lastActive = time;
                    active->active = false;
                    //modified by zxy
                    //cache->updateBGPentry(active, bgpMessage->prefixPeer, time);
                    cache->updateBGPentry(active, bgpMessage->prefixPeer, time, producer);
                    //end of modify
                }
                prefixPath->lastChange = time;
                prefixPath->lastActive = time;
                prefixPath->active = true;
                newPath = prefixPath;
                //modified by zxy
                //cache->updateBGPentry(prefixPath,bgpMessage->prefixPeer, time);
                cache->updateBGPentry(prefixPath,bgpMessage->prefixPeer, time, producer);
                //end of modify
                
            } else {
                //this path already exists
                if (active != NULL){
                    if (active == prefixPath){ //active Path and current path are the same
                        // the path is already active ! This is an AADup
                        prefixPath->AADup++;
                        bgpMessage->category = AADup;
                        //modified by zxy
                        //cache->updateBGPentry(prefixPath, bgpMessage->prefixPeer, time);
                        cache->updateBGPentry(prefixPath, bgpMessage->prefixPeer, time, producer);
                        //end of modify
                    } else {
                        //This is an implicit withdrawal and replacement with different
                        withdraw = true;
                        active->AADiff++;
                        active->meanUp = active->coeff*active->meanUp+
                                         (1-active->coeff)*(time-active->lastActive);
                        active->lastChange = time;
                        active->active =false;
                        //modified by zxy
                        //cache->updateBGPentry(active, bgpMessage->prefixPeer, time);
                        //cache->updateBGPentry(prefixPath, bgpMessage->prefixPeer, time);
                        cache->updateBGPentry(active, bgpMessage->prefixPeer, time, producer);
                        cache->updateBGPentry(prefixPath, bgpMessage->prefixPeer, time, producer);
                        //end of modify
                    }
                } else {
                    // There is no active path
                    if (bgpMessage->timestamp - previous->lastChange < 300) {
                        //This is a flap
                        prefixPath->Flap++;
                        bgpMessage->category = Flap;
                    } else {
                        //this a WADup explicit withdrawal and replacement with identic
                        prefixPath->WADup++;
                        bgpMessage->category = WADup;
                    }
                    prefixPath->meanDown = prefixPath->coeff * prefixPath->meanDown +
                                     (1 - prefixPath->coeff) * (time - prefixPath->lastChange);
                    prefixPath->lastChange =time;
                    prefixPath->active = true;
                    //modified by zxy
                    //cache->updateBGPentry(prefixPath, bgpMessage->prefixPeer, time);
                    cache->updateBGPentry(prefixPath, bgpMessage->prefixPeer, time, producer);
                    //end of modify
                }
                newPath = prefixPath;
            }
            collectorOutage = updateBestPath(bgpMessage, active, newPath, withdraw);
        }
    } else if (bgpMessage->type == BGPSTREAM_ELEM_TYPE_WITHDRAWAL){
        if (active != NULL){
            bgpMessage->category = Withdrawn;
            active->announcementNum++;
            //there is already an active path
            active->active = false;
            active->meanUp = active->coeff*active->meanUp+
                             (1-active->coeff)*(time-active->lastActive);
            active->lastChange = time;
            withdraw = true;

            collectorOutage= updateBestPath(bgpMessage, active, NULL, withdraw);
            //modified by zxy
            //cache->updateBGPentry(active, bgpMessage->prefixPeer, time);
            cache->updateBGPentry(active, bgpMessage->prefixPeer, time, producer);
            //end of modify
        } else {
            if (prefixPath != NULL){
                bgpMessage->category= WWDup;
                prefixPath ->WWDup++;
            }
        }
    }
    return collectorOutage;
}

bool Table::updateBestPath(BGPMessage *bgpMessage,PrefixPath *active, PrefixPath *newpath, bool withdraw) {
    PrefixElement *prefixElement;
    unsigned int asn;
    bool test, found = false;
    concurrent_hash_map<std::string, CollectorElement*>::const_accessor acc;
    concurrent_hash_map<std::string, PrefixElement*>::const_accessor acc1;


    bool collectorOutage = false;
    string str = bgpMessage->prefixPeer->str();
    if (withdraw) {
        asn = active->path->shortPath.back();
    } else {
        asn = newpath->path->shortPath.back();
    }
    AS *as = cache->asCache.find(asn)->second;
    if ((bgpMessage->prefixPeer->getVersion() == BGPSTREAM_ADDR_VERSION_IPV4) &&
        ((version == 4) || (version == 64))) {
        collectorElements.find(acc, bgpMessage->collector);
        if (acc->second->prefixElements.find(acc1, bgpMessage->prefixPeer->str())){
            found = true;
        }
    } else if ((bgpMessage->prefixPeer->getVersion() == BGPSTREAM_ADDR_VERSION_IPV6) &&
               ((version == 6) || (version == 64))) {
//        it = collectorElements.find(bgpMessage->collector)->second->prefixElements.find(bgpMessage->prefixPeer->str());
    }
    if (found){
        prefixElement = acc1->second;
        if (withdraw) {
            if (active != NULL) {
                if (prefixElement->bestPath == active) {
                    // best path of collector is withdraw
                    collectorOutage = prefixElement->setBestPath();
                }
            }
        }
        if (bgpMessage->prefixPath != NULL){
            //it is an update
            if (bgpMessage->category != AADup){
                //this is not a duplicate announcement
                prefixElement->addPath(bgpMessage->prefixPath,bgpMessage->timestamp);
            }
            collectorOutage = prefixElement->setBestPath();
        }
    } else{
        collectorOutage = true;
    }
    return collectorOutage;
}


BGPMessage *BGPTables::update(BGPMessage *bgpMessage,ProducerKafka* producer){
    map<string, CollectorElement *>::iterator itV4;
    map<string, CollectorElement *>::iterator itV6;
    map<string, PrefixElement *>::iterator it;
    concurrent_hash_map<std::string, CollectorElement*>::const_accessor acc;
    concurrent_hash_map<std::string, PrefixElement*>::const_accessor acc1;

    Table * table;

    bool status;

    if ((version == 4 ) || (version == 64)){
        if (!tableV4->collectorElements.find(acc,bgpMessage->collector)){
            CollectorElement *collectorElement = new CollectorElement(bgpMessage->collector);
            tableV4->collectorElements.insert(pair<string, CollectorElement *>(bgpMessage->collector, collectorElement));
        }
    }
    if ((version == 6 ) || (version == 64)){
        if (!tableV6->collectorElements.find(acc,bgpMessage->collector)){
            CollectorElement *collectorElement = new CollectorElement(bgpMessage->collector);
            tableV6->collectorElements.insert(pair<string, CollectorElement* >(bgpMessage->collector, collectorElement));

        }
    }
    table =tableV4;
    if ((bgpMessage->prefixPeer->getVersion() == BGPSTREAM_ADDR_VERSION_IPV4) && ((version == 4 ) || (version == 64))){
        table = tableV4;
    } else if ((bgpMessage->prefixPeer->getVersion() == BGPSTREAM_ADDR_VERSION_IPV6) && ((version == 6 ) || (version == 64))){
        table = tableV6;
    }
    //modified by zxy
    //status= table->update(bgpMessage);
    status= table->update(bgpMessage,producer);
    //end of modify
    
    if (status){
        // A prefix is withdrawn from a collector
        bool found = false;
        for (auto collectorElem:table->collectorElements){
            concurrent_hash_map<std::string, PrefixElement*> *p= &collectorElem.second->prefixElements;
            if (p->find(acc1, bgpMessage->prefixPeer->str()) && (acc1->second->bestPath != NULL)){
                found =true;
                break;
            }
        }
        if (!found){
            //There is a global prefix withdraw
            cache->removePrefixPeer(bgpMessage->prefixPeer,bgpMessage->peerASNumber,bgpMessage->activePrefixPath->path->shortPath.back(),bgpMessage->timestamp, producer);
        }
    }
    return bgpMessage;
    //updateEventTable(bgpMessage);
}

void TableFlagger::run(){
    int status;
    BGPMessage *bgpMessage;

     while(true){
        if (infifo.try_take(bgpMessage, std::chrono::milliseconds(240000))==BlockingCollectionStatus::TimedOut){
            cout<< "timeout"<<endl;
            break;
        } else {
//            if (bgpMessage->type ==BGPSTREAM_ELEM_TYPE_ANNOUNCEMENT )
//                cout<<"ANN: "<<bgpMessage->timestamp<<","<< bgpMessage->collector<<","<<infifo.size()<<","<<outfifo.size()<<","<<bgpMessage->path->prefixPeerSet.size()<<","<<bgpMessage->path->pathString<<","<<bgpMessage->prefixPeer->str<<endl;
//            else{
//                cout<<"WIT: "<<bgpMessage->timestamp<<","<< bgpMessage->collector<<","<<infifo.size()<<","<<outfifo.size()<<","<<bgpMessage->prefixPeer->peerSet.size()<<","<<bgpMessage->prefixPeer->str<<endl;
//            }
            
            //modified by zxy
            //bgpMessage = bgpTables->update(bgpMessage);
            bgpMessage = bgpTables->update(bgpMessage,producer);
            //end of modify
            
            bgpSource->returnBGPMessage(bgpMessage);
            outfifo.add(bgpMessage);
        }
    }
    cout<< "Data Famine Table"<<endl;

}

TableFlagger::TableFlagger(PriorityBlockingCollection<BGPMessage *,  PriorityContainer<BGPMessage *, BGPMessageComparer>>
                           &infifo, BlockingCollection<BGPMessage *> &outfifo, BGPTables *bgpTables, BGPCache *cache,
                           BGPSource *bgpSource, int version, ProducerKafka *producer): cache(cache), infifo(infifo), outfifo(outfifo),
                           version(version),bgpSource(bgpSource), bgpTables(bgpTables), producer(producer){}
