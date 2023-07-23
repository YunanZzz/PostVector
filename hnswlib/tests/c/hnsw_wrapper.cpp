#include "hnsw_wrapper.h"
#include "../../hnswlib/hnswlib.h"

HnswIndex hnsw_new() {
    return new hnswlib::HierarchicalNSW<float>(new hnswlib::L2Space(16), 10000, 16, 200);
}

void hnsw_addPoint(HnswIndex index, const float* datapoint, int label) {
    static_cast<hnswlib::HierarchicalNSW<float>*>(index)->addPoint(datapoint, label);
}

void hnsw_searchKnn(HnswIndex index, const float* query_data, int k, int* labels, float* distances) {
    auto results = static_cast<hnswlib::HierarchicalNSW<float>*>(index)->searchKnn(query_data, k);
    printf("1:%d\n",results.top().second);
    results.pop();
    printf("2:%d\n",results.top().second);
    results.pop();

    // for (int i = 0; i < k; i++) {
    //     printf("%f\n",labels[i]);
    //     labels[i] = results.top().second;
    //     //distances[i] = results.top().first;
    //     printf("%f\n",labels[i]);
    //     results.pop();
    // }
}

void hnsw_saveIndex(HnswIndex index, const char* path) {
    static_cast<hnswlib::HierarchicalNSW<float>*>(index)->saveIndex(path);
}

// void hnsw_loadIndex(HnswIndex index, const char* path) {
//     static_cast<hnswlib::HierarchicalNSW<float>*>(index)->loadIndex(path);
// }
void hnsw_loadIndex(HnswIndex index, const char* path) {
    hnswlib::SpaceInterface<float> *s = new hnswlib::L2Space(16);
    size_t max_elements_i = 16;
    static_cast<hnswlib::HierarchicalNSW<float>*>(index)->loadIndex(path, s, max_elements_i);
}


void hnsw_delete(HnswIndex index) {
    delete static_cast<hnswlib::HierarchicalNSW<float>*>(index);
}
