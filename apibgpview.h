//
// Created by Kave Salamatian on 2018-12-26.
//

#ifndef BGPGEOPOLITICS_APIBGPVIEW_H
#define BGPGEOPOLITICS_APIBGPVIEW_H
#include "BGPGeopolitics.h"
#include <curl/curl.h> 
#include "json.hpp" 
#include <sqlite3.h>

class AS;
class APIbgpview{
    public :
    sqlite3 *db;
    BGPCache *cache;
    APIbgpview(sqlite3 *db, BlockingCollection<AS *> &infifo, BGPCache *cache): db(db), cache(cache), infifo(infifo){}
    void insert(AS *as);
    void update(AS *as);
    void updateBGPView(AS *as);
    void insertDB(AS *as);
    void updateDB(AS *as);
    void run();
private:
    CURL *curl;
    CURLcode res;
    string url="";
    sqlite3_stmt *stmt;
    BlockingCollection<AS *> &infifo;
    
};

#endif //BGPGEOPOLITICS_APIBGPVIEW_H
