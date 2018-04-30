/**
* DirectedTriangleCount
* @author cuixuange
* @version 1.0
*/

#include <stdio.h>
#include <string.h>
#include <math.h>
#include <iostream>
#include "GraphLite.h"

#define VERTEX_CLASS_NAME(name) DirectedTriangleCount##name


using namespace std;


typedef struct {
    int64_t inTriangle = 0;
    int64_t outTriangle = 0;
    int64_t throughTriangle = 0;
    int64_t circleTriangle = 0;

    vector <int64_t> inVidList;      //avoid double free
    vector <int64_t> outVidList;

} VertexType;


class VERTEX_CLASS_NAME(InputFormatter) : public InputFormatter {
public:
    int64_t getVertexNum() {
        unsigned long long n;
        sscanf(m_ptotal_vertex_line, "%lld", &n);
        m_total_vertex = n;
        return m_total_vertex;
    }

    int64_t getEdgeNum() {
        unsigned long long n;
        sscanf(m_ptotal_edge_line, "%lld", &n);
        m_total_edge = n;
        return m_total_edge;
    }

    int getVertexValueSize() {
        m_n_value_size = sizeof(VertexType);        //change
        return m_n_value_size;
    }

    int getEdgeValueSize() {
        m_e_value_size = sizeof(int64_t);
        return m_e_value_size;
    }

    int getMessageValueSize() {
        m_m_value_size = sizeof(int64_t);
        return m_m_value_size;
    }

    void loadGraph() {
        VertexType curVertex;
        bzero(&curVertex, sizeof(curVertex));
        unsigned long long last_vertex;
        unsigned long long from;
        unsigned long long to;
        int64_t weight = 0;
        int64_t outdegree = 0;

        //add the first item
        const char *line = getEdgeLine();
        sscanf(line, "%lld %lld", &from, &to);
        addEdge(from, to, &weight);
        last_vertex = from;
        ++outdegree;
        curVertex.outVidList.push_back(to);

        for (int64_t i = 1; i < m_total_edge; ++i) {
            line = getEdgeLine();
            sscanf(line, "%lld %lld", &from, &to);
            if (last_vertex != from) {
                addVertex(last_vertex, &curVertex, outdegree);
                last_vertex = from;
                outdegree = 1;
                bzero(&curVertex, sizeof(curVertex));
                //update out_vid_list
                curVertex.outVidList.push_back(to);
            } else {
                curVertex.outVidList.push_back(to);
                ++outdegree;
            }
            addEdge(from, to, &weight);
        }
        addVertex(last_vertex, &curVertex, outdegree);
    }
};

class VERTEX_CLASS_NAME(OutputFormatter) : public OutputFormatter {
public:
    void writeResult() {
        int64_t vid;
        char s[1024];
        VertexType vertex;
        for (ResultIterator r_iter; !r_iter.done(); r_iter.next()) {
            r_iter.getIdValue(vid, &vertex);
            printf("vertex.inVidList.size() = %lu\n", vertex.inVidList.size());
            if (vertex.inVidList.size() != 0) {
                int n = sprintf(s, "in: %ld\nout: %ld\nthrough: %ld\ncircle: %ld\n",
                                vertex.inTriangle, vertex.outTriangle, vertex.throughTriangle, vertex.circleTriangle);
                writeNextResLine(s, n);
                bzero(&vertex, sizeof(vertex));
                break;
            }
            // sizeof(item) not sizeof(struct)
            //iterator.next() = move sizeof(Node::n_size)
            bzero(&vertex, sizeof(vertex));
        }
    }
};

// An aggregator that records a double value tom compute sum
class VERTEX_CLASS_NAME(Aggregator) : public Aggregator<int64_t> {
public:
    void init() {
        m_global = 0;
        m_local = 0;
    }

    void *getGlobal() {
        return &m_global;
    }

    void setGlobal(const void *p) {
        m_global = *(int *) p;
    }

    void *getLocal() {
        return &m_local;
    }

    void merge(const void *const p) {     //FIXME: why not const const?
        m_global += *(int *) p;
    }

    void accumulate(const void *const p) {
        m_local += *(int *) p;
    }
};


////Vertex Value Type Edge Value Type Message Value Type
class VERTEX_CLASS_NAME() : public Vertex<VertexType, int64_t, int64_t> {
public:
    void compute(MessageIterator *pmsgs) {

        int64_t in_triangle = 0;
        int64_t out_triangle = 0;


        VertexType curVertex;
        //store current vertex.in/out_vid_list
        curVertex.inVidList.assign(getValue().inVidList.begin(), getValue().inVidList.end());
        curVertex.outVidList.assign(getValue().outVidList.begin(), getValue().outVidList.end());

        if (getSuperstep() == 0) {
            sendMessageToAllNeighbors(getVertexId());
        } else {
            //update vertex in_vid_list
            if (getSuperstep() == 1) {
                int64_t curOutDegree = Node::getNode(getVertexId()).m_out_degree;
                for (; !pmsgs->done(); pmsgs->next()) {
                    int64_t neighbor_id = pmsgs->getValue();
                    curVertex.inVidList.push_back(neighbor_id);
                }
                //change current vertex value
                *mutableValue() = curVertex;

                for (int64_t in_vid : curVertex.inVidList) {
                    sendMessageToAllNeighbors(in_vid);
                }
                voteToHalt();
            }
            else if (getSuperstep() == 2) {
                int64_t through_triangle = 0;
                int64_t circle_triangle = 0;
                for (; !pmsgs->done(); pmsgs->next()) {
                    //from neighbors's in_vid
                    int64_t msg_vid = pmsgs->getValue();
                    for (int64_t my_inVid : getValue().inVidList) {
                        if (my_inVid == msg_vid) {
                            through_triangle++;
                        }
                    }
                    for (int64_t my_outVid : getValue().outVidList) {
                        if (my_outVid == msg_vid) {
                            circle_triangle++;
                        }
                    }
                }
                curVertex.throughTriangle = through_triangle;
                curVertex.circleTriangle = circle_triangle;

                accumulateAggr(0, &through_triangle);
                accumulateAggr(1, &circle_triangle);
                *mutableValue() = curVertex;
                sendMessageToAllNeighbors(getVertexId());
                voteToHalt();
            }
            else if (getSuperstep() == 3) {
                int64_t through = *(int64_t *) getAggrGlobal(0);
                int64_t circle = *(int64_t *) getAggrGlobal(1);

                curVertex.inTriangle = through;
                curVertex.outTriangle = through;
                curVertex.throughTriangle = through;
                curVertex.circleTriangle = circle / 3;

                *mutableValue() = curVertex;
                voteToHalt();
                return;
            }
            else{
                return;
            }
        }
    }
};

class VERTEX_CLASS_NAME(Graph) : public Graph {
public:
    VERTEX_CLASS_NAME(Aggregator) *aggregator;

public:
    // argv[0]: PageRankVertex.so
    // argv[1]: <input path>
    // argv[2]: <output path>
    void init(int argc, char *argv[]) {

        setNumHosts(5);
        setHost(0, "localhost", 1411);
        setHost(1, "localhost", 1421);
        setHost(2, "localhost", 1431);
        setHost(3, "localhost", 1441);
        setHost(4, "localhost", 1451);

        if (argc < 3) {
            printf("Usage: %s <input path> <output path>\n", argv[0]);
            exit(1);
        }

        m_pin_path = argv[1];
        m_pout_path = argv[2];

        aggregator = new VERTEX_CLASS_NAME(Aggregator)[2];
        regNumAggr(2);
        regAggr(0, &aggregator[0]);
        regAggr(1, &aggregator[1]);
    }

    void term() {
        delete[] aggregator;
    }
};

/* STOP: do not change the code below. */
extern "C" Graph *create_graph() {
    Graph *pgraph = new VERTEX_CLASS_NAME(Graph);

    pgraph->m_pin_formatter = new VERTEX_CLASS_NAME(InputFormatter);
    pgraph->m_pout_formatter = new VERTEX_CLASS_NAME(OutputFormatter);
    pgraph->m_pver_base = new VERTEX_CLASS_NAME();

    return pgraph;
}

extern "C" void destroy_graph(Graph *pobject) {
    delete (VERTEX_CLASS_NAME() *) (pobject->m_pver_base);
    delete (VERTEX_CLASS_NAME(OutputFormatter) *) (pobject->m_pout_formatter);
    delete (VERTEX_CLASS_NAME(InputFormatter) *) (pobject->m_pin_formatter);
    delete (VERTEX_CLASS_NAME(Graph) *) pobject;
}

