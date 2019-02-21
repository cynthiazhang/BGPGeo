//
// Created by Kave Salamatian on 2018-12-01.
//

#include <sqlite3.h>
#include <set>
#include <map>
#include <boost/range/combine.hpp>
#include "BGPGraph.h"
#include "BGPGeopolitics.h"

//using boost::get;
using namespace std;
using namespace tbb;




int BGPCache::fillASCache() {
    sqlite3_stmt *stmt;
    int asn;
    string name;
    string country;
    string RIR;
    float risk;
    int count = 0;
    string sql;

    cout << "Starting fill AS Cache" << endl;
    /* Create SQL statement */
    sql = "SELECT asNumber, name, country, RIR, riskIndex, geoRisk, perfRisk, secuRisk, otherRisk, observed FROM asn";
    /* Execute SQL statement */
    sqlite3_prepare(db, sql.c_str(), sql.length(), &stmt, NULL);
    bool done = false;
    while (!done) {
        switch (sqlite3_step(stmt)) {
            case SQLITE_ROW: {
                asn = atoi((const char *) sqlite3_column_text(stmt, 0));
                name = string((const char *) sqlite3_column_text(stmt, 1));
                country = string((const char *) sqlite3_column_text(stmt, 2));
                RIR = string((const char *) sqlite3_column_text(stmt, 3));
                risk = atof((const char *) sqlite3_column_text(stmt, 4));
                AS *as = new AS(asn, name, country, RIR, risk);
//                    cout << asn << "," << name << "," << country << "," << RIR << "," << risk << endl;
                asCache.insert(pair<int, AS*>(asn, as));
                count++;
                break;
            }
            case SQLITE_DONE: {
                done = true;
                break;
            }
        }
    }
    sqlite3_reset(stmt);
    sqlite3_finalize(stmt);
    cout << count << " row processed" << endl;
//    cout << contents << endl;
//        updateAS(174, asCache[174]);
    return 0;
}

AS *BGPCache::updateAS(int asn, AS *as) {
    if (as == NULL){
        as = new AS(asn, "XX", "XX", "XX", 0.0);
        toAPIbgpbiew.add(as);
    } else {
        toAPIbgpbiew.add(as);
    }
    return as;
}

AS *BGPCache::chkAS(unsigned int asn, unsigned int time){
    AS *as;
    map<unsigned int, AS*>::iterator it;
    map<int, boost::graph_traits<Graph>::vertex_descriptor >::iterator it1;
    concurrent_hash_map<int,boost::graph_traits<Graph>::vertex_descriptor>::accessor acc;
    concurrent_hash_map<int,boost::graph_traits<Graph>::vertex_descriptor>::const_accessor acc1;



    it = asCache.find( asn );
    if (it == asCache.end()) {
        as = updateAS(asn, NULL);
        asCache.insert(pair<unsigned int, AS *>(asn, as));
    } else {
        as = it->second;
    }
    as->observed = true;
    as->cTime = time;
    bool found = bgpg->asnToVertex.find(acc, as->asNum);
    if (found) acc.release();
    if (!found){
        boost::graph_traits<Graph>::vertex_descriptor v = bgpg->add_vertex(VertexP{to_string(as->asNum), as->country, as->name, as->cTime, 0, 0});
        bgpg->asnToVertex.insert(pair<int,boost::graph_traits<Graph>::vertex_descriptor >(as->asNum, v));
    }
    return as;
}


void BGPCache::updateBGPentry(PrefixPath* path, PrefixPeer * prefixPeer, unsigned int time, ProducerKafka *producer){
    if (path->active)
        setPathActive(path,prefixPeer, time, producer);
    else
        setPathNonActive(path,prefixPeer, time, producer);
}


void BGPCache::setPathActive(PrefixPath *prefixPath, PrefixPeer * prefixPeer, unsigned int time,ProducerKafka *producer){
    AS *as, *as1;
    unsigned int tmp, peer, asn;
    map<unsigned int, AS *>::iterator it;
    map<string, Link *>::iterator it2;
    map<string, PrefixPath *>::iterator it3;
    map<string, PrefixPeer *>::iterator it4;
    concurrent_unordered_set<PrefixPeer *>::const_iterator it5;
    map<int, boost::graph_traits<Graph>::vertex_descriptor>::iterator it6;
    concurrent_hash_map<int, PrefixPath  *>::accessor acc;
    concurrent_hash_map<int, PrefixPath  *>::accessor acc1;
    concurrent_hash_map<int,boost::graph_traits<Graph>::vertex_descriptor>::accessor acc2;
    concurrent_hash_map<string, PrefixPeer *>::const_accessor acc3;
    concurrent_hash_map<string, Link *>::const_accessor acc4;


    Link *link;
    Path *path = prefixPath->path;
    asn = path->shortPath.back();
    peer = path->shortPath.front();
    string peerStr = prefixPath->collector +'|'+to_string(peer);
    int peerStrIndex =  peerStrMap.insert(peerStr);
    string str = prefixPeer->str();
    string peerPrefixStr = peerStr+'|'+str;
    int peerPrefixIndex =  peerPrefixStrMap.insert(peerPrefixStr);
    string pathPrefixStr = prefixPath->path->pathString() + '|' + peerPrefixStr;
    if (prefixPath->path->pathString()=="[]"){
        int j=0;
    }
    int pathPrefixIndex = pathPrefixStrMap.insert(pathPrefixStr);


    if (inactivesMap.find(acc1, pathPrefixIndex)){
        inactivesMap.erase(acc1);
    }
    acc1.release();
    activesMap.insert(acc, pair<int, PrefixPath *>(peerPrefixIndex, prefixPath));
    acc.release();
    it = asCache.find( asn );
    as = it->second;
    if (!as->activePrefixMap.find(acc3, str)){
        as->activePrefixMap.insert(pair<string, PrefixPeer*>(str, prefixPeer));
        as->activePrefixNum = as->activePrefixMap.size() ;
        as->allPrefixNum =  as->activePrefixNum+ as->inactivePrefixSet.size();
        if (prefixPeer->addAS(as->asNum)>1){
            if (prefixPeer->checkHijack()){
                as->status |= HIJACKING;
                for (auto it = prefixPeer->asSet.begin();it != prefixPeer->asSet.end();it++){
                    if (*it != as->asNum){
                        as1 = asCache.find(*it)->second;
                        as1->status |= HIJACKED;
                    }
                }
                //modified by zxy
                //eventTable->checkHijack(as, prefixPeer, time);
                eventTable->checkHijack(as, prefixPeer, time, producer);
                //end of modify
            }
        }
        as->inactivePrefixSet.erase(prefixPeer);
    } else {
        prefixPeer = acc3->second;
    }
    acc3.release();
    prefixPeer->addPeer(peerStrIndex, time);
//    prefixPeer->addAS(as->asNum);
    vector<unsigned int> srcVect(path->shortPath);
    vector<unsigned int> dstVect(path->shortPath);
    srcVect.pop_back();
    dstVect.erase(dstVect.begin());
    for (const auto & zipped : boost::combine(srcVect, dstVect)) {
        int src,dst;
        boost::tie(src, dst) = zipped;
        if (src>dst){
            tmp = src;
            src = dst;
            dst = tmp;
        }
        AS *srcAS= asCache.find(src)->second;
        AS *dstAS= asCache.find(dst)->second;
        string linkId= to_string(src)+"|"+to_string(dst);
        Link *link;
        if (!linksMap.find(acc4, linkId)){
            link = new Link(src, dst, time);
            linksMap.insert(pair<string, Link *>(linkId,link ));
        } else {
            link = acc4->second;
        }
        if (!link->activePrefixMap.find(acc3, str)){
            link->activePrefixMap.insert(pair<string, PrefixPeer*>(str, prefixPeer));
        }
        acc3.release();
        prefixPeer->addPeer(peerStrIndex,time);
        link->cTime = time;
        boost::graph_traits<Graph>::vertex_descriptor v0, v1;
        v0=0;
        if (bgpg->asnToVertex.find(acc2, src)){
            v0=acc2->second;
            acc.release();
        }
        v1=0;
        if (bgpg->asnToVertex.find(acc2, dst)){
            v1=acc2->second;
            acc2.release();
        }
        if (bgpg->set_edge(v0,v1, EdgeP{(long)link->activePrefixMap.size(), 1, link->cTime})) {
            srcAS->reconnect();
            dstAS->reconnect();
        }
    }
    if ((as->status & OUTAGE) && !(as->checkOutage())){
        //modified by zxy
        //eventTable->checkOutage(as,NULL, time);
        eventTable->checkOutage(as, NULL, time, producer);
        //end of add
    }
    boost::graph_traits<Graph>::vertex_descriptor v;
    if (bgpg->asnToVertex.find(acc2, asn)){
        v = acc2->second;
    }
    acc1.release();
    bgpg->set_vertex(v,VertexP{to_string(as->asNum), as->country, as->name, time, (int)as->activePrefixNum, (int)as->allPrefixNum });
}

void BGPCache::setPathNonActive(PrefixPath *prefixPath, PrefixPeer *prefixPeer, unsigned int time, ProducerKafka *producer){
    AS *as, *as1;
    unsigned int tmp, peer, asn;
    map<unsigned int, AS*>::iterator it;
    map<string,Link *>::iterator it2;
    map<string,PrefixPath *>::iterator it3;
    map<string, PrefixPeer *>::iterator it4;
    set<bgpstream_pfx_storage_t *>::iterator it5;
    concurrent_hash_map<int, PrefixPath  *>::accessor acc;
    concurrent_hash_map<int, PrefixPath  *>::accessor acc1;
    concurrent_hash_map<int,boost::graph_traits<Graph>::vertex_descriptor>::accessor acc2;
    concurrent_hash_map<string, PrefixPeer *>::const_accessor acc3;
    concurrent_hash_map<string, Link *>::const_accessor acc4;


    Path *path = prefixPath->path;
    asn = path->shortPath.back();
    peer = path->shortPath.front();
    string peerStr = prefixPath->collector + '|' + to_string(peer);
    short int peerStrIndex =  peerStrMap.insert(peerStr);
    string str = prefixPeer->str();
    string peerPrefixStr= peerStr + '|' + str;
    int peerPrefixIndex =  peerPrefixStrMap.insert(peerPrefixStr);
    string pathPrefixStr = path->pathString()+ '|' + peerPrefixStr;
    int pathPrefixIndex = pathPrefixStrMap.insert(pathPrefixStr);
    
    if (activesMap.find(acc, peerPrefixIndex)){
       inactivesMap.insert(acc1,pair<int, PrefixPath* >(pathPrefixIndex, acc->second));
       activesMap.erase(acc);
    }
    acc.release();
    acc1.release();
    vector<unsigned int> srcVect(path->shortPath);
    vector<unsigned int> dstVect(path->shortPath);
    srcVect.pop_back();
    dstVect.erase(dstVect.begin());
    for (const auto & zipped : boost::combine(srcVect, dstVect)) {
        int src, dst;
        boost::tie(src, dst) = zipped;
        if (src > dst) {
            tmp = src;
            src = dst;
            dst = tmp;
        }
        AS *srcAS= asCache.find(src)->second;
        AS *dstAS= asCache.find(dst)->second;

        string linkId = to_string(src) + "|" + to_string(dst);
        linksMap.find(acc4,linkId);
        Link *link = acc4->second;

        if (link->activePrefixMap.find(acc3, str)){
            link->activePrefixMap.insert(pair<string,PrefixPeer *>(str, prefixPeer));
        }
        acc3.release();
        if (prefixPeer->removePeer(peerStrIndex, time) == 0) {
            link->activePrefixMap.erase(str);
        }
        link->cTime = time;
        boost::graph_traits<Graph>::vertex_descriptor v0, v1;
        bgpg->asnToVertex.find(acc2,src);
        v0 = acc2->second;
        acc2.release();
        bgpg->asnToVertex.find(acc2,dst);
        v1 = acc2->second;
        acc2.release();
        if (link->activePrefixMap.size() == 0){
            bgpg->remove_edge(v0, v1);
            bgpg->asnToVertex.find(acc2,asn);
            boost::graph_traits<Graph>::vertex_descriptor v = acc2->second;
            acc2.release();
            if (bgpg->in_degree(v0)==0) {
                srcAS->disconnect();
            }
            if (bgpg->in_degree(v1)==0) {
                dstAS->disconnect();
            }
        } else{
            boost::graph_traits<Graph>::edge_descriptor e;
            bgpg->set_edge(v0,v1, EdgeP{(long)link->activePrefixMap.size(), 1, link->cTime});
        }
    }
    as = asCache.find(asn)->second;
    if (prefixPeer->removePeer(peerStrIndex,time) == 0) {
        as->activePrefixMap.erase(str);
        if ((as->status & HIJACKING) && (prefixPeer->removeAS(as->asNum)<2)) {
            if (!prefixPeer->checkHijack()) {
                as->status &= ~HIJACKING;
                for (auto it = prefixPeer->asSet.begin();it != prefixPeer->asSet.end();it++){
                    as1 = asCache.find(*it)->second;
                    if (as1->status & HIJACKED){
                        as1->status &= ~HIJACKED;
                    }
                }
            }
            //modified by zxy
            //eventTable->checkHijack(as, prefixPeer, time);
            eventTable->checkHijack(as, prefixPeer, time, producer);
            //end of modify
        }
        as->inactivePrefixSet.insert(prefixPeer);
        as->activePrefixNum = as->activePrefixMap.size();
        if (as->checkOutage()){
            //modified by zxy
            //eventTable->checkOutage(as,prefixPeer, time);
            eventTable->checkOutage(as,prefixPeer, time, producer);
            //end of modify
            
        }
//        removePrefixPeer(prefixPeer,peer,asn,time);
    }
    as->cTime = time;
    bgpg->asnToVertex.find(acc2,asn);
    boost::graph_traits<Graph>::vertex_descriptor v = acc2->second;
    acc2.release();
    bgpg->set_vertex(v,VertexP{to_string(as->asNum), as->country, as->name, time, (int)as->activePrefixNum, (int)as->allPrefixNum } );
}


void BGPCache::removePrefixPeer(PrefixPeer *prefixPeer,unsigned int peer, unsigned int asn,unsigned int time,  ProducerKafka *producer){
    //remove the prefixPeer from the AS
    concurrent_hash_map<string, PrefixPeer *>::const_accessor acc;


    string str = prefixPeer->str();
    AS *as = asCache.find(asn)->second, *as1;
    if (as->activePrefixMap.find(acc, str)){
        PrefixPeer *prefixPeer = acc->second;
        as->inactivePrefixSet.insert(prefixPeer);
        as->activePrefixMap.erase(str);
        if (prefixPeer->removeAS(as->asNum)<2){
            if (as->status & HIJACKING){
                if (!prefixPeer->checkHijack()){
                    as->status &= ~HIJACKING;
                    for (auto it = prefixPeer->asSet.begin();it != prefixPeer->asSet.end();it++){
                        as1 = asCache.find(*it)->second;
                        if (as1->status & HIJACKED){
                            as1->status &= ~HIJACKED;
                        }
                    }
                }
                //modified by zxy
                //eventTable->checkHijack(as, prefixPeer, time);
                eventTable->checkHijack(as, prefixPeer, time, producer);
                //end of modify
            }
        }
        if (as->checkOutage()){
            //modified by zxy
            //eventTable->checkOutage(as, prefixPeer, time);
            eventTable->checkOutage(as, prefixPeer, time, producer);
            //end of modify
        }
    }
}

long BGPCache::size_of(){
    long size1=0,size2=0,size3=0, size4=0, size5=0;
    int len1=prefixPeerMap.size();
    for (auto p:prefixPeerMap){
        size1 += p.first.length()+p.second->size_of()+4;
    }
    int len2=pathsMap.size();
    for (auto p:pathsMap){
        size2 += p.first.length()+p.second->size_of()+4;
    }
    int len3=activesMap.size();
    for (auto p:activesMap){
        size3 += 4+p.second->size_of()+4;
    }
    int len4=inactivesMap.size();
    for (auto p:inactivesMap){
        size4 += 4+p.second->size_of()+4;
    }
    int len5=asCache.size();
    for (auto p:asCache) {
        size5 += 4+p.second->size_of();
    }
    return size1+size2+size3+size4+size5;
}


