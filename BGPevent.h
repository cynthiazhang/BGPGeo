//
// Created by Kave Salamatian on 2018-12-13.
//
#ifndef BGPGEOPOLITICS_BGPEVENT_H
#define BGPGEOPOLITICS_BGPEVENT_H
#include "BGPGeopolitics.h"
#include "BGPKafkaProducer.hpp"
#include <sstream>

using namespace std;
//using namespace boost;  

enum EventType{OUTAGE_EV, HIJACK_EV};

class AS;
class PrefixPeer;

class Event{
public:
    unsigned int asn;
    AS *as;
    unsigned int bTime=0, eTime=0;
    int involvedPrefixNum;
    int allPrefixNum;
    EventType type;
    set<PrefixPeer *> involvedPrefixSet;
    Event(EventType type, AS *as, unsigned int time);
    void addPrefix(PrefixPeer *prefixPeer){
        involvedPrefixSet.insert(prefixPeer);
    }

};

class HijackEvent: public Event{
public:
    unsigned int hijackedASn;
    HijackEvent(AS *hijackingAS, unsigned int hijackedASn, unsigned time): Event(HIJACK_EV, hijackingAS, time),
    hijackedASn(hijackedASn){}
};

class OutageEvent: public Event{
public:
    OutageEvent(AS *as, unsigned int time): Event(OUTAGE_EV,as,time){}
};

class Events{
public:
    unsigned int asNum;
    vector<Event *> activeEvents;
    vector<Event *> inactiveEvents;
    Events(unsigned int asNum): asNum(asNum){}
};

class EventTable{
public:
    map<unsigned int, Events *> eventASMap;
    //modified by zxy
    //void checkOutage(AS *as,  PrefixPeer *prefixPeer, unsigned int time);
    //void checkHijack(AS *as, PrefixPeer *prefixPeer, unsigned int time);
    void checkOutage(AS *as,  PrefixPeer *prefixPeer, unsigned int time, ProducerKafka *producer);
    void checkHijack(AS *as, PrefixPeer *prefixPeer, unsigned int time, ProducerKafka *producer);
};
#endif //BGPGEOPOLITICS_BGPEVENT_H
