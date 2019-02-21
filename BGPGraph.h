//
// Created by Kave Salamatian on 26/11/2018.
//

#ifndef BGPGEOPOLITICS_BGPGRAPH_H
#define BGPGEOPOLITICS_BGPGRAPH_H
#include <iostream>                  // for std::cout
#include <stdio.h> 
#include <stdlib.h>
#include <boost/graph/use_mpi.hpp>
#include <boost/graph/distributed/mpi_process_group.hpp>
#include <boost/graph/use_mpi.hpp>
#include <boost/graph/distributed/mpi_process_group.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/filesystem.hpp>
#include <boost/iostreams/device/file.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/bzip2.hpp>
#include <boost/graph/copy.hpp>
#include "tbb/concurrent_hash_map.h"
#include <boost/thread/shared_mutex.hpp>




#include <boost/graph/adjacency_list.hpp>
//#include <boost/property_map/property_map.hpp>
//#include <boost/graph/dijkstra_shortest_paths.hpp>
//#include <boost/graph/edge_list.hpp>
//#include <string>
#include <boost/property_map/property_map.hpp>
#include <boost/graph/graphml.hpp>
#include <boost/numeric/ublas/matrix.hpp>
#include <boost/numeric/ublas/io.hpp>
//#include <boost/graph/floyd_warshall_shortest.hpp>
#include <boost/graph/johnson_all_pairs_shortest.hpp>
#include <boost/graph/exterior_property.hpp>


using namespace std;
using namespace boost;
using namespace tbb;


struct VertexP {
    string asn;
    string country;
    string name;
    unsigned int time;
    int prefixNum;
    int prefixAll;
}; //bundled property map for nodes

struct EdgeP {
    long count;
    int weight;
    unsigned int time;
//    EdgeProperties(): count(0){}
//    EdgeProperties(int givencount): count(givencount){}
};

struct GraphP {
    string name;
};



typedef adjacency_list<setS, vecS, undirectedS, VertexP, EdgeP, GraphP> Graph;
typedef boost::graph_traits < Graph >::adjacency_iterator adjacency_iterator;
typedef boost::property_map<Graph, boost::vertex_index_t>::type IndexMap;



class BGPGraph{
public:
    Graph g;
    dynamic_properties  dp;
    concurrent_hash_map<int,boost::graph_traits<Graph>::vertex_descriptor> asnToVertex;

    IndexMap index;

    BGPGraph(){
        dp.property("asNumber", get(&VertexP::asn, g));
        dp.property("Country", get(&VertexP::country, g));
        dp.property("Name", get(&VertexP::name, g));
        dp.property("asTime", get(&VertexP::time, g));
        dp.property("prefixNum", get(&VertexP::prefixNum, g));
        dp.property("prefixAll", get(&VertexP::prefixAll, g));
        dp.property("count", get(&EdgeP::count, g));
        dp.property("edgeTime",get(&EdgeP::time, g));
        dp.property("weight",get(&EdgeP::weight, g));

        index = get(boost::vertex_index, g);
    }

    void copy(Graph &gcopy){
        boost::shared_lock<boost::shared_mutex> lock(mutex_);
        copy_graph( g, gcopy);
    }

    void save(std::string outfile){
        Graph g1;
        boost::iostreams::filtering_ostream out;
        out.push(boost::iostreams::gzip_compressor());
        out.push( boost::iostreams::file_descriptor_sink(outfile+".gz"));
        copy(g1);

        write_graphml(out, g1, dp, true);
    }

    boost::graph_traits<Graph>::vertex_descriptor add_vertex(VertexP vertexP){
        boost::unique_lock<boost::shared_mutex> lock(mutex_);
        return boost::add_vertex(vertexP, g);
    }

    boost::graph_traits<Graph>::edge_descriptor  add_edge(boost::graph_traits<Graph>::vertex_descriptor v0, boost::graph_traits<Graph>::vertex_descriptor v1, EdgeP edgeP){
        return boost::add_edge(v0, v1, edgeP, g).first;
    }

    bool set_edge(boost::graph_traits<Graph>::vertex_descriptor v0, boost::graph_traits<Graph>::vertex_descriptor v1, EdgeP edgeP){
        boost::graph_traits<Graph>::edge_descriptor e;
        boost::unique_lock<boost::shared_mutex> lock(mutex_);

        pair<boost::graph_traits<Graph>::edge_descriptor, bool> p= boost::edge(v0, v1, g);
        if (!p.second){
            e = add_edge(v0, v1, edgeP);
            return true;
        } else {
            e = p.first;
            g[e].count = edgeP.count;
            g[e].weight = edgeP.weight;
            g[e].time = edgeP.time;
            return false;
        }
    }

    void set_vertex(boost::graph_traits<Graph>::vertex_descriptor v, VertexP vertexP){
        boost::unique_lock<boost::shared_mutex> lock(mutex_);
        g[v].prefixNum = vertexP.prefixNum;
        g[v].prefixAll = vertexP.prefixAll;
        g[v].time = vertexP.time;
    }

    void get_vertex(boost::graph_traits<Graph>::vertex_descriptor v, VertexP &vertexP){
        boost::shared_lock<boost::shared_mutex> lock(mutex_);
        vertexP.asn = g[v].asn;
        vertexP.country = g[v].country;
        vertexP.name = g[v].name;
        vertexP.time = g[v].time;
        vertexP.prefixNum = g[v].prefixNum;
        vertexP.prefixAll = g[v].prefixAll;
    }

    void remove_edge(boost::graph_traits<Graph>::vertex_descriptor v0, boost::graph_traits<Graph>::vertex_descriptor v1){
        boost::unique_lock<boost::shared_mutex> lock(mutex_);
        boost::remove_edge(v0, v1, g);
    }

    int  in_degree(boost::graph_traits<Graph>::vertex_descriptor v){
        boost::shared_lock<boost::shared_mutex> lock(mutex_);
        return boost::in_degree(v,g);
    }


private:
    mutable boost::shared_mutex mutex_;

};



#endif //BGPGEOPOLITICS_BGPGRAPH_H
