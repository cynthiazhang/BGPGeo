//
// Created by Kave Salamatian on 2018-12-01.
//
#include "BGPGeopolitics.h"
#include "cache.h"
using namespace tbb;  

BGPMessage::BGPMessage(BGPCache *cache, int order): cache(cache), poolOrder(order){}

//BGPMessage::BGPMessage(bgpstream_elem_t *elem, std::string collector, BGPCache *cache): cache(cache){
//    fill(elem,collector, cache);
//}

void BGPMessage::fill(long order, bgpstream_elem_t *elem, unsigned int time, std::string incollector, BGPCache *cache) {
    bgpstream_as_path_iter_t iter;
    bgpstream_as_path_seg_t *seg;
    unsigned int asn;
    std::map<std::string, Path *>::iterator it1;
    Path *path;
    AS *as;
    double asRisk=0.0;
    concurrent_hash_map<string, Path *>::const_accessor acc;


    messageOrder = order;
    category = None;
    pathString = "[";
    prefixPath = NULL;
    activePrefixPath = NULL;
    previousPrefixPath = NULL;
    newPath = false;
    asPath.clear();
    shortPath.clear();
    type = elem->type;
//    timestamp = elem->timestamp;
    
    timestamp = time;
    collector = incollector;

//    memcpy(&peerAddress, &elem->peer_address, sizeof(bgpstream_addr_storage_t));
    memcpy(&peerAddress, &elem->peer_ip , sizeof(bgpstream_addr_storage_t));

//    peerASNumber = elem->peer_asnumber;
    peerASNumber = elem->peer_asn;
    memcpy(&nextHop, &elem->nexthop, sizeof(bgpstream_addr_storage_t));

    std::map<std::string, PrefixPeer *>::iterator it;
    char buf[20];
    bgpstream_pfx_snprintf(buf, 20, (bgpstream_pfx_t *) &elem->prefix);
    std::string str(buf);

    it = cache->prefixPeerMap.find(str);
    if (it == cache->prefixPeerMap.end()) {
        prefixPeer = new PrefixPeer(&elem->prefix, timestamp);
        cache->prefixPeerMap.insert(pair<std::string, PrefixPeer *>(prefixPeer->str(), prefixPeer));
    } else {
        prefixPeer = it->second;
    }
    bgpstream_as_path_iter_reset(&iter);
//        asPath = new std::vector<unsigned int>();
    int i = 0;
//    while ((seg = bgpstream_as_path_get_next_seg(elem->aspath, &iter)) != NULL) {
    while ((seg = bgpstream_as_path_get_next_seg(elem->as_path, &iter)) != NULL) {
        switch (seg->type) {
            case BGPSTREAM_AS_PATH_SEG_ASN:
                asn = ((bgpstream_as_path_seg_asn_t *) seg)->asn;
                asPath.push_back(asn);
                break;
            case BGPSTREAM_AS_PATH_SEG_SET:
                /* {A,B,C} */
                break;
            case BGPSTREAM_AS_PATH_SEG_CONFED_SET:
                /* [A,B,C] */
                break;
            case BGPSTREAM_AS_PATH_SEG_CONFED_SEQ:
                /* (A B C) */
                break;
        }
    }
    shortenPath();
    if (!cache->pathsMap.find(acc, pathString)) {
        path = new Path(this);
        path->prefixPeerSet.insert(prefixPeer);
        cache->pathsMap.insert(pair<std::string, Path *>(pathString, path));
    } else {
        path = acc->second;
        path->prefixPeerSet.insert(prefixPeer);
    }
    path->risk=0.0;
    for (int i = 0; i < path->shortPath.size(); i++) {
        asn = path->shortPath[i];
        as = cache->chkAS(asn, timestamp);
//        path->geoPath.push_back(as->country);
//        path->asNames.push_back(as->name);
        asRisk= fusionRisks(as->geoRisk, as->secuRisk,  as->otherRisk);
        if (asRisk > path->risk){
            path->risk = asRisk ;
        }
    }

}

void BGPMessage::shortenPath() {
    unsigned int prev = 0;
    for (auto asn:asPath) {
        if (asn != prev) {
            shortPath.push_back(asn);
            pathString += to_string(asn)+",";
            prev = asn;
        }
    }
    pathString += "]";
}

void BGPMessage::preparePath(){
    Path *path;
    concurrent_hash_map<int, PrefixPath  *>::accessor acc;
    concurrent_hash_map<int, PrefixPath  *>::accessor acc1;
    concurrent_hash_map<string, Path *>::const_accessor acc2;

    int asn;
    int MaxRisk =0;
    AS * as;

    string str = prefixPeer->str();
    string peerPrefixStr = collector+ '|' + std::to_string(peerASNumber) + '|' + str;
    int peerPrefixIndex =  cache->peerPrefixStrMap.insert(peerPrefixStr);

    if ((type == BGPSTREAM_ELEM_TYPE_ANNOUNCEMENT) || (type == BGPSTREAM_ELEM_TYPE_RIB)) {
        string pathPrefixStr = pathString + '|' + peerPrefixStr;
        int pathPrefixIndex = cache->pathPrefixStrMap.insert(pathPrefixStr);
        activePrefixPath = NULL;
//        cout<<"SIZE:"<<cache->activesMap.size()<<endl;
        if (cache->activesMap.find(acc,peerPrefixIndex)) {
            //There is already an active path for this prefix
            if (acc->second->active) {
                activePrefixPath = acc->second;
                if (activePrefixPath->path->pathString() == pathString){
                    // The input path is active
                    previousPrefixPath = activePrefixPath;
                } else {
                    //the active path was another one
                    if (cache->inactivesMap.find(acc1, pathPrefixIndex)){
                        // the input path was active but not anymore
                        previousPrefixPath = acc1->second;
                    }
                    else {
                        //this path has never been announced
                        previousPrefixPath = NULL;
                    }
                    acc1.release();
                }
            }
        } else {
            //There is no active path for this prefix
            if (cache->inactivesMap.find(acc1, pathPrefixIndex)){
                // The given path was active but not anymore
                previousPrefixPath = acc1->second;
            }
            else {
                //this path has never been announced
                previousPrefixPath = NULL;
            }
            acc1.release();
        }
        acc.release();
        if (previousPrefixPath == NULL){
            //this is a new path
            cache->pathsMap.find(acc2, pathString);
            path = acc2->second;
            path->prefixPeerSet.insert(prefixPeer);
            prefixPath  = new PrefixPath(collector, path, prefixPeer);
            prefixPath->active = true;
            newPath = true;
        } else {
            prefixPath = previousPrefixPath;
            prefixPath->active = true;
        }
    } else if(type == BGPSTREAM_ELEM_TYPE_WITHDRAWAL){
        activePrefixPath= NULL;
        if(cache->activesMap.find(acc,peerPrefixIndex)){
            if (acc->second->active) {
                activePrefixPath = acc->second;
            }
        }
        acc.release();
    }
}

double BGPMessage::fusionRisks(double geoRisk, double secuRisk, double otherRisk){
    double stdRisk=1.0;
    if (stdRisk==0) {
        return 0;
    } else
        return secuRisk/(stdRisk*0.5+geoRisk*0.5);
}


int PrefixPath::size_of(){
    int size=0;
//    size += prefixPeer->size_of();
    size += collector.length();
//    size += path->size_of();
    size += 10*4+5*8;
    return size;
}

double PrefixPath::getScore() const {
    return path->pathLength*1.0;
}

int Path::size_of(){
    int size=0;
    size += 8 + shortPath.size()*4;
    for (auto p:prefixPeerSet){
        size += p->size_of();
    }
    return size;
}

