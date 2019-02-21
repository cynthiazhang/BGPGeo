//
// Created by Kave Salamatian on 2018-12-27.
//
#include "BGPGeopolitics.h"
#include <curl/curl.h>
#include "json.hpp"
#include <sqlite3.h>




using namespace std;
using json = nlohmann::json;  


string contents;
size_t handle_data(void *ptr, size_t size, size_t nmemb, void *stream)
{
    int numbytes = size*nmemb;
    // The data is not null-terminated, so get the last character, and replace
    // it with '\0'.
    char lastchar = *((char *) ptr + numbytes - 1);
    *((char *) ptr + numbytes - 1) = '\0';
    contents.append((char *)ptr);
    contents.append(1,lastchar);
    *((char *) ptr + numbytes - 1) = lastchar;  // Might not be necessary.
    return size*nmemb;
}

void APIbgpview::insert(AS *as){
    updateBGPView(as);
    insertDB(as);
}

void APIbgpview::update(AS *as){
    updateBGPView(as);
    updateDB(as);
}

void APIbgpview::updateBGPView(AS *as) {
    int asn = as->asNum;
    contents = "";
    bool insert = false;
    concurrent_hash_map<int,boost::graph_traits<Graph>::vertex_descriptor>::accessor acc;
    curl = curl_easy_init();
    if (curl) {
        url = "http://api.bgpview.io/asn/" + to_string(asn);
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        /* example.com is redirected, so we tell libcurl to follow redirection */
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, handle_data);
        /* Perform the request, res will get the return code */
        res = curl_easy_perform(curl);
        /* Check for errors */
        if (res != CURLE_OK)
            fprintf(stderr, "curl_easy_perform() failed: %s\n",
                    curl_easy_strerror(res));
        /* always cleanup */
        curl_easy_cleanup(curl);
    }
    try {
        json j = json::parse(contents);
        if (!j["data"]["name"].is_null()) {
            as->name = j["data"]["name"];
        } else {
            as->name = "UNKNOWN";
        }
        if (!j["data"]["country_code"].is_null()) {
            as->country = j["data"]["country_code"];
        } else {
            as->country = "XX";
        }
        if (!j["data"]["rir_allocation"]["rir_name"].is_null()) {
            as->RIR = j["data"]["rir_allocation"]["rir_name"];
        } else {
            as->RIR = "UNKNOWN";
        }
    } catch (json::parse_error) {
        cout << "JSON Error :" << asn << "," << contents << endl;
    }
    boost::graph_traits<Graph>::vertex_descriptor v0;
    VertexP vertexP;

    if (cache->bgpg->asnToVertex.find(acc, asn)) {
        v0 = acc->second;
        acc.release();
        cache->bgpg->get_vertex(v0, vertexP);
        vertexP.country = as->country;
        vertexP.name = as->name;
        cache->bgpg->set_vertex(v0,vertexP);
    }
}

void APIbgpview::insertDB(AS *as){
    string sql = "INSERT INTO asn VALUES (" + to_string(as->asNum) + ",\"" + as->name + "\",\"" +
                 as->country + "\",\"" + as->RIR + "\"," + to_string(0.0) + "," + to_string(0.0) + "," +
                 to_string(0.0) +
                 "," + to_string(0.0) + "," + to_string(0.0) + "," + to_string(1)+")";
    sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, NULL);//preparing the statement
    sqlite3_step(stmt);//executing the statement
    sqlite3_finalize(stmt);
}
void APIbgpview::updateDB(AS *as){
    string sql = "UPDATE asn SET country =\"" + as->country + "\" ,name=\"" + as->name + "\" WHERE asNumber=" +
                 to_string(as->asNum);
    sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, NULL);//preparing the statement
    sqlite3_step(stmt);//executing the statement
    sqlite3_finalize(stmt);
}

void APIbgpview::run(){
    AS *as;
    while(true){
        infifo.take(as);
        if (as->asNum==0){
            break;
        } else {
            if (as->name=="XX") {
                insert(as);
            } else{
                update(as);
            }
        }
    }
}
