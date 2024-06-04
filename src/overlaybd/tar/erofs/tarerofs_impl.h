#include "erofs/tar.h"
#include "erofs/tarerofs_api.h"
#include <photon/fs/filesystem.h>

class TarErofsInter::TarErofsImpl {
public:
    TarErofsImpl(photon::fs::IFile *file, photon::fs::IFile *target, uint64_t fs_blocksize = 4096,
          photon::fs::IFile *bf = nullptr, bool meta_only = true, bool first_layer = true)
        : file(file), fout(target), fs_base_file(bf), meta_only(meta_only), first_layer(first_layer) {

            io_manager_img.dev_open = dev_open_img;
            io_manager_img.dev_read = dev_read_img;
            io_manager_img.dev_write = dev_write_img;
            io_manager_img.dev_resize = dev_resize_img;
            io_manager_img.dev_fillzero = dev_fillzero_img;
            io_manager_img.dev_close = dev_close_img;
            io_manager_img.private_data = (void*)this;

            tar_io_manager.read = read_tar;
            tar_io_manager.private_data = (void*)this;

            io_manager_base.dev_open_ro = dev_open_ro_base;
            io_manager_base.dev_read = dev_read_base;
            io_manager_base.dev_close = dev_close_base;
            io_manager_base.private_data = (void*)this;
        }

    int extract_all();

public:
    photon::fs::IFile *file = nullptr;     // source
    photon::fs::IFile *fout = nullptr; // target
    photon::fs::IFile *fs_base_file = nullptr;
    bool meta_only;
    bool first_layer;
    struct erofs_io_manager io_manager_img;
    struct erofs_tarerofs_io_manager tar_io_manager;
    struct erofs_io_manager io_manager_base;
public:
    /* io functions for the output image */
    static int dev_open_img(struct erofs_sb_info *sbi, const char *devname);
    static int dev_read_img(struct erofs_sb_info *sbi, int device_id, void *buf, u64 offset, size_t len);
    static int dev_write_img(struct erofs_sb_info *sbi, const void *buf, u64 offset, size_t len);
    static int dev_resize_img(struct erofs_sb_info *sbi, erofs_blk_t nblocks);
    static int dev_fillzero_img(struct erofs_sb_info *sbi, u64 offset,
		size_t len, bool padding);
    static void dev_close_img(struct erofs_sb_info *sbi);

    /* io functions for tar file*/
    static u64 read_tar(void *buf, u64 bytes, void *data);

    /* io functions for the base image in the --base mode */
    static int dev_open_ro_base(struct erofs_sb_info *sbi, const char *dev);
    static int dev_read_base(struct erofs_sb_info *sbi, int device_id,
	    void *buf, u64 offset, size_t len);
    static void dev_close_base(struct erofs_sb_info *sbi);
};