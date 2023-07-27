#include "hnsw_wrapper.h"
#include <stdlib.h>  // for rand function
#include <time.h>    // for time function

#define DIMENSIONS 128
#define NUM_POINTS 10000
#define K 10

int main() {
    srand(time(NULL));  // initialize the random seed

    // create a new HNSW index
    HnswIndex index = hnsw_new();

    // allocate memory for the data
    float (*data)[DIMENSIONS] = malloc(sizeof(float[DIMENSIONS]) * NUM_POINTS);

    // generate random data and add to the index
    for (int i = 0; i < NUM_POINTS; ++i) {
        for (int j = 0; j < DIMENSIONS; ++j) {
            data[i][j] = (float)rand() / (float)RAND_MAX;
        }
        hnsw_addPoint(index, data[i], i);
    }

    // allocate memory for the labels and distances
    int (*labels)[K] = malloc(sizeof(int[K]) * NUM_POINTS);
    float (*distances)[K] = malloc(sizeof(float[K]) * NUM_POINTS);

    // search for the K nearest neighbors of each point
    for (int i = 0; i < NUM_POINTS; ++i) {
        hnsw_searchKnn(index, data[i], K, labels[i], distances[i]);
    }

    hnsw_saveIndex(index, "index.bin");
    hnsw_delete(index);

    free(data);
    free(labels);
    free(distances);

    return 0;
}
void test(){
    srand(time(NULL));  // initialize the random seed

    // create a new HNSW index
    HnswIndex index = hnsw_new();

    // allocate memory for the data
    float (*data)[DIMENSIONS] = malloc(sizeof(float[DIMENSIONS]) * NUM_POINTS);

    // generate random data and add to the index
    for (int i = 0; i < NUM_POINTS; ++i) {
        for (int j = 0; j < DIMENSIONS; ++j) {
            data[i][j] = (float)rand() / (float)RAND_MAX;
        }
        hnsw_addPoint(index, data[i], i);
    }

    // allocate memory for the labels and distances
    int (*labels)[K] = malloc(sizeof(int[K]) * NUM_POINTS);
    float (*distances)[K] = malloc(sizeof(float[K]) * NUM_POINTS);

    // search for the K nearest neighbors of each point
    for (int i = 0; i < NUM_POINTS; ++i) {
        hnsw_searchKnn(index, data[i], K, labels[i], distances[i]);
    }

    hnsw_saveIndex(index, "index.bin");
    hnsw_delete(index);

    free(data);
    free(labels);
    free(distances);




}
// void test(){
// 	int dim = 16;               // Dimension of the elements
//     int max_elements = 10000;   // Maximum number of elements, should be known beforehand
//     int M = 16;                 // Tightly connected with internal dimensionality of the data
//                                 // strongly affects the memory consumption
//     int ef_construction = 200;  // Controls index search speed/build speed tradeoff

//     // Initing index
//     hnswlib::L2Space space(dim);
//     hnswlib::HierarchicalNSW<float>* alg_hnsw = new hnswlib::HierarchicalNSW<float>(&space, max_elements, M, ef_construction);

//     // Generate random data
//     std::mt19937 rng;
//     rng.seed(47);
//     std::uniform_real_distribution<> distrib_real;
//     float* data = new float[dim * max_elements];
//     for (int i = 0; i < dim * max_elements; i++) {
//         data[i] = distrib_real(rng);
//     }

//     // Add data to index
//     for (int i = 0; i < max_elements; i++) {
//         alg_hnsw->addPoint(data + i * dim, i);
//     }

//     // Query the elements for themselves and measure recall
//     float correct = 0;
//     for (int i = 0; i < max_elements; i++) {
//         std::priority_queue<std::pair<float, hnswlib::labeltype>> result = alg_hnsw->searchKnn(data + i * dim, 1);
//         hnswlib::labeltype label = result.top().second;
//         if (label == i) correct++;
//     }
//     float recall = correct / max_elements;
//     std::cout << "Recall: " << recall << "\n";

//     // Serialize index
//     std::string hnsw_path = "hnsw.bin";
//     alg_hnsw->saveIndex(hnsw_path);
//     delete alg_hnsw;

//     // Deserialize index and check recall
//     alg_hnsw = new hnswlib::HierarchicalNSW<float>(&space, hnsw_path);
//     correct = 0;
//     for (int i = 0; i < max_elements; i++) {
//         std::priority_queue<std::pair<float, hnswlib::labeltype>> result = alg_hnsw->searchKnn(data + i * dim, 1);
//         hnswlib::labeltype label = result.top().second;
//         if (label == i) correct++;
//     }
//     recall = (float)correct / max_elements;
//     std::cout << "Recall of deserialized index: " << recall << "\n";

//     delete[] data;
//     delete alg_hnsw;
// }