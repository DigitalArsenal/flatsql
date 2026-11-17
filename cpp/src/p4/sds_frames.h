#ifndef FLATSQL_P4_SDS_FRAMES_H
#define FLATSQL_P4_SDS_FRAMES_H
// Built from the released SDS schema bytes, with distinct generated guards.
#include "FMB/main_generated.h"
#include "FCK/main_generated.h"
#include "FSG/main_generated.h"
#include <algorithm>
#include <map>
#include <vector>
#include "frames.h"
namespace flatsql { namespace p4 {
// The verifier options of a stream frame ($FMB, $FCK, $FSG) of n bytes. A
// table takes at least 4 bytes, so the frame holds at most n/4 tables.
// FlatBuffers' default bound of 1,000,000 tables refuses a valid checkpoint
// segment with more entries (a dictionary of over a million object keys is
// one table per key), and a refused segment reads as a corrupt feed.
inline flatbuffers::Verifier::Options frameVerifierOptions(size_t n) {
    flatbuffers::Verifier::Options o;
    o.max_tables=flatbuffers::uoffset_t(std::max<size_t>(o.max_tables,n/4+1));
    return o;
}
// An FMB side table (SEALED, LARGE, SIGNATURES, IDENTITIES) indexed by ROW
// once per frame; empty when the table is absent. False on a ROW outside the
// frame's rows or a repeated ROW.
template<class T> bool fmbByRow(const flatbuffers::Vector<flatbuffers::Offset<T>>* table,size_t rows,std::vector<const T*>* out) {
    out->clear(); if(!table) return true; out->assign(rows,nullptr);
    for(const auto* x:*table) { if(x->ROW()>=rows || (*out)[x->ROW()]) return false; (*out)[x->ROW()]=x; }
    return true;
}
template<class T> const T* fmbRow(const std::vector<const T*>& byRow,size_t row) { return row<byRow.size()?byRow[row]:nullptr; }
bool verifyFmbCommit(const ::FMB*,uint64_t,uint64_t,uint32_t,const std::map<uint64_t,FmbFrameProof>&);
} }
#endif
