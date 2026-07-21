#ifndef FLATSQL_SDN_NODE_H
#define FLATSQL_SDN_NODE_H

#ifdef __cplusplus
extern "C" {
#endif

int append_records(void);
int query_records(void);
int configure_index(void);
int upsert_view(void);
int compact(void);
int configure_retention(void);
int snapshot(void);
int reload(void);

#ifdef __cplusplus
}
#endif

#endif  // FLATSQL_SDN_NODE_H
