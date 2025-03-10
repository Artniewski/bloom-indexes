#pragma once

#include <bitset>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "bloom_value.hpp"

class node {
 public:

    std::vector<node*> children;
    bloom_value blValue;
    std::string filename;

    node(bloom_value bv, std::string filename){
        this->blValue= bv;
        this->filename=filename;
    }

    node()
    {
       filename="Memory";
    }

    void addchildren(int pos, int ratio, std::vector<node*> nodes);
};