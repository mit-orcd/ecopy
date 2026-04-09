#ifndef TRAVERSAL_H
#define TRAVERSAL_H
int traversal_start(const char *src_dir, const char *dst_dir);
void traversal_wait(void);
int traversal_finalize_metadata(void);
int traversal_status(void);
#endif
