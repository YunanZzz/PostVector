#ifndef HNSW_WRAPPER_H
#define HNSW_WRAPPER_H

#ifdef __cplusplus
extern "C" {
#endif

typedef void* HnswIndex;

HnswIndex hnsw_new();
void hnsw_addPoint(HnswIndex index, const float* datapoint, int label);
void hnsw_searchKnn(HnswIndex index, const float* query_data, int k, int* labels, float* distances);
void hnsw_saveIndex(HnswIndex index, const char* path);
void hnsw_loadIndex(HnswIndex index, const char* path);
void hnsw_delete(HnswIndex index);

#ifdef __cplusplus
}
#endif

#endif // HNSW_WRAPPER_H
