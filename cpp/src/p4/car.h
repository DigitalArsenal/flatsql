#ifndef FLATSQL_P4_CAR_H
#define FLATSQL_P4_CAR_H
#include <cstdint>
#include <string>
namespace flatsql { namespace p4 {
struct Feed; struct Type; struct Internal;
struct CarStats { uint64_t bytes=0,blocks=0,liveStreamBytes=0,wallNs=0; };
// Calls run on the type's writer, including the before-export checkpoint.
// Restore requires the registered type and an absent
// destination feed; its stream is replayed by the ordinary recovery writer.
int32_t carExportFeed(Feed*,const std::string& path,CarStats*,std::string* err);
int32_t carRestoreFeed(Type*,const std::string& path,Feed** restored,CarStats*,std::string* err);
void carWork(Type*,int op,Internal*);
} }
#endif
