#include "hnsw_wrapper.h"
#include "../../hnswlib/hnswlib.h"

HnswIndex hnsw_new(int dim, int max_num,int bnn, int efb) {
    return new hnswlib::HierarchicalNSW<float>(new hnswlib::L2Space(dim), max_num, bnn, efb);
}

void hnsw_addPoint(HnswIndex index, const float* datapoint, int label) {
    static_cast<hnswlib::HierarchicalNSW<float>*>(index)->addPoint(datapoint, label);
}
void hnsw_setEf(HnswIndex index, int ef) {
    static_cast<hnswlib::HierarchicalNSW<float>*>(index)->setEf(ef);
}

void hnsw_searchKnn(HnswIndex index, const float* query_data, int k, int* labels, float* distances) {
    auto results = static_cast<hnswlib::HierarchicalNSW<float>*>(index)->searchKnn(query_data, k);
    //  printf("1:%d\n",results.top().second);
    //  printf("2:%f\n",results.top().first);
    // results.pop();
    // printf("2:%d\n",results.top().second);
    // results.pop();

    for (int i = 0; i < k; i++) {
        labels[i] = results.top().second;
        distances[i] = results.top().first;
        results.pop();
    }
}

void hnsw_saveIndex(HnswIndex index, const char* path) {
    static_cast<hnswlib::HierarchicalNSW<float>*>(index)->saveIndex(path);
}


void hnsw_loadIndex(HnswIndex index, const char* path, int dim) {
    hnswlib::SpaceInterface<float> *s = new hnswlib::L2Space(dim);
    static_cast<hnswlib::HierarchicalNSW<float>*>(index)->loadIndex(path, s);
}


void hnsw_delete(HnswIndex index) {
    delete static_cast<hnswlib::HierarchicalNSW<float>*>(index);
}
