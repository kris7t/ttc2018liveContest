#pragma once

#include <stdexcept>
#include <vector>
#include <chrono>

extern "C" {
#include <GraphBLAS.h>
#include <LAGraph.h>
}

struct BenchmarkParameters {
    std::string ChangePath;
    std::string RunIndex;
    int Sequences;
    std::string Tool;
    std::string ChangeSet;
    std::string Query;
    unsigned long thread_num = 1;
};

BenchmarkParameters parse_benchmark_params();

using score_type = std::tuple<uint64_t, time_t, GrB_Index>;

struct BenchmarkPhase {
    static const std::string Initialization;
    static const std::string Load;
    static const std::string Initial;
    static const std::string Update;
};

void report(const BenchmarkParameters &parameters, int iteration, const std::string &phase,
            std::chrono::nanoseconds runtime,
            std::optional<std::vector<score_type>> result_reversed_opt = std::nullopt);

void WriteOutDebugMatrix(const char *title, GrB_Matrix result);

void WriteOutDebugVector(const char *title, GrB_Vector result);

/*
 * GRAPHBLAS HELPER FUNCTION
 */

//------------------------------------------------------------------------------
// ok: call a GraphBLAS method and check the result
//------------------------------------------------------------------------------

// ok(GrB_Info) is a function that processes result of a GraphBLAS method and checks the status;
// if a failure occurs, it returns the error status to the caller.

void inline ok(GrB_Info info) {
    if (info != GrB_SUCCESS) {
        throw std::runtime_error{std::string{"GraphBLAS error: "} + GrB_error()};
    }
}
