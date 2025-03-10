#include "node.hpp"

void node::addchildren(int i, int ratio, std::vector<node*> nodes){
    int j=i;

    int size = static_cast<int>(nodes.size());
    while(j<i+ratio && j< size){
        this->children.push_back(nodes[j]);
        j++;
    }
}



