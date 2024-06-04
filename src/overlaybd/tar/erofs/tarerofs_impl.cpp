#include "tarerofs_interface.h"
#include "tarerofs_impl.h"
#include "erofs/tarerofs_api.h"
#include "erofs/tar.h"
#include "erofs/io.h"
#include "../../lsmt/file.h"
#include "../../lsmt/index.h"
#include <photon/common/alog.h>

#define TAREROFS_BLOCK_SIZE 4096
#define TAREROFS_BLOCK_BITS 12
#define DATA_OFFSET 1073741824
#define MIN_RW_LEN 512ULL
#define round_down_blk(addr) (addr & (~(MIN_RW_LEN - 1)))
#define round_up_blk(addr) (round_down_blk(addr + MIN_RW_LEN - 1))
#define MAP_FILE_NAME "upper.map"

/*
 * Helper function for reading from the photon file, since
 * the photon file requires reads to be 512-byte aligned.
 */
static size_t read_photon_file(void *buf, u64 offset, size_t len, photon::fs::IFile *file)
{
    size_t ret;
    u64 start, end;
    char *big_buf;

    start = round_down_blk(offset);
    end = round_up_blk(offset + len);

    big_buf = (char *)malloc(end - start);
    if (!big_buf)
        return -1;
    
    ret = file->pread(big_buf, end - start, start);
    if (ret != end - start) {
        free(big_buf);
        return -1;
    }

    memcpy(buf, big_buf + offset - start, len);
    return len;
}

/*
 * Helper function for writing to a photon file.
 */
static size_t write_photon_file(const void *buf, u64 offset, size_t len, photon::fs::IFile *file)
{
    size_t ret;
    u64 start, end;
    char *big_buf;

    start = round_down_blk(offset);
    end = round_up_blk(offset + len);

    if (start != offset || end != offset + len) {
        big_buf = (char *)malloc(end - start);
        if (!big_buf)
            return -1;
         /* writes within a sector range */
        if (end - start == MIN_RW_LEN) {
            ret = file->pread(big_buf, MIN_RW_LEN, start);
            if (ret != MIN_RW_LEN) {
                free(big_buf);
                return -1;
            }
        } else {
            /* 
             * writes that span at least two sectors,
             * we read the head and tail sectors in such case
             */
            if (start != offset) {
                ret = file->pread(big_buf, (size_t)MIN_RW_LEN, start);
                if (ret != MIN_RW_LEN) {
                    free(big_buf);
                    return -1;
                }
            }

            if (end != offset + len) {
                ret = file->pread(big_buf + end - start - MIN_RW_LEN,
                                  (size_t)MIN_RW_LEN,
                                  (off_t)end - MIN_RW_LEN);
                if (ret != MIN_RW_LEN) {
                    free(big_buf);
                    return -1;
                }
            }
        }

        memcpy(big_buf + offset - start, buf, len);
        len = end - start;
        ret = file->pwrite(big_buf, len, start);
        free(big_buf);
    } else {
        ret = file->pwrite(buf, len, offset);
    }
    
    if ((size_t)ret != len)
        return -1;

    return 0;
}


int TarErofsInter::TarErofsImpl::dev_open_img(struct erofs_sb_info *sbi, const char *devname)
{
    TarErofsImpl *obj = static_cast<TarErofsImpl*>(sbi->io_manager->private_data);
    photon::fs::IFile *fout = obj->fout;

    fout->lseek(0, 0);
    sbi->devsz = INT64_MAX;
    return 0;
}

int TarErofsInter::TarErofsImpl::dev_read_img(struct erofs_sb_info *sbi, int device_id, void *buf, u64 offset, size_t len)
{
    int read_count;
    TarErofsImpl *obj = static_cast<TarErofsImpl*>(sbi->io_manager->private_data);
    photon::fs::IFile *fout = obj->fout;

    if (read_photon_file(buf, offset, len, fout) != len)
        return -1;

    return 0;
}


int TarErofsInter::TarErofsImpl::dev_write_img(struct erofs_sb_info *sbi, const void *buf,
	      u64 offset, size_t len)
{
    size_t ret;
    u64 start, end;
    char *big_buf;

    TarErofsImpl *obj = static_cast<TarErofsImpl*>(sbi->io_manager->private_data);
    photon::fs::IFile *fout = obj->fout;

    if (!buf)
            return -EINVAL;
    offset += sbi->diskoffset;

    if (offset >= sbi->devsz || len > sbi->devsz ||
            offset > sbi->devsz - len) {
            return -EINVAL;
    }

    return write_photon_file(buf, offset, len, fout);
}

static u64 length;
int TarErofsInter::TarErofsImpl::dev_resize_img(struct erofs_sb_info *sbi, erofs_blk_t nblocks)
{
    length = (u64)nblocks * erofs_blksiz(sbi);
    length += sbi->diskoffset;

    return 0;
}

int TarErofsInter::TarErofsImpl::dev_fillzero_img(struct erofs_sb_info *sbi, u64 offset,
		 size_t len, bool padding)
{
    static const char zero[EROFS_MAX_BLOCK_SIZE] = {0};
	int ret;

    while (len > erofs_blksiz(sbi)) {
        ret = erofs_dev_write(sbi, zero, offset, erofs_blksiz(sbi));
        if (ret)
            return ret;
        len -= erofs_blksiz(sbi);
        offset += erofs_blksiz(sbi);
    }

    return erofs_dev_write(sbi, zero, offset, len);
}

void TarErofsInter::TarErofsImpl::dev_close_img(struct erofs_sb_info *sbi)
{
    return;
}

u64 TarErofsInter::TarErofsImpl::read_tar(void *buf, u64 bytes, void *data)
{
    TarErofsImpl *obj = static_cast<TarErofsImpl*>(data);

    u64 i = 0;
    while (bytes) {
        u64 len = bytes > INT_MAX ? INT_MAX : bytes;
        u64 ret;

        ret = obj->file->read(buf + i, len);
        if (ret < 1) {
            if (ret == 0)
                break;
            else
                return -1;
        }
        bytes -= ret;
        i += ret;
    }
    return i;
}

int TarErofsInter::TarErofsImpl::dev_open_ro_base(struct erofs_sb_info *sbi, const char *dev)
{
    sbi->devsz = INT64_MAX;
    return 0;
}

int TarErofsInter::TarErofsImpl::dev_read_base(struct erofs_sb_info *sbi, int device_id,
	    void *buf, u64 offset, size_t len)
{
    TarErofsImpl *obj = static_cast<TarErofsImpl*>(sbi->io_manager->private_data);
    photon::fs::IFile *fs_base_file = obj->fs_base_file;

    if (read_photon_file(buf, offset, len, fs_base_file) != len)
        return -1;

    return 0;
}

void TarErofsInter::TarErofsImpl::dev_close_base(struct erofs_sb_info *sbi)
{
    return;
}

static int init_sbi(struct erofs_sb_info *sbi,
                    struct erofs_io_manager *io_manager)
{
    int err;
    struct timeval t;

    sbi->blkszbits = TAREROFS_BLOCK_BITS;    
    err = gettimeofday(&t, NULL);
    if (err) 
        return err;
    sbi->build_time = t.tv_sec;
    sbi->build_time_nsec = t.tv_usec;
    sbi->io_manager = io_manager;
    err = erofs_dev_open(sbi, "");

    return err;
}

static int init_tarerofs(struct erofs_tarfile *erofstar,
                         photon::fs::IFile *tar_file,
                         struct erofs_tarerofs_io_manager *tar_io_manager)
{
    int err;
    struct stat st;

    erofstar->global.xattrs = LIST_HEAD_INIT(erofstar->global.xattrs);
    erofstar->index_mode = true;
    erofstar->mapfile = MAP_FILE_NAME;
    erofstar->aufs = true;
    err = tar_file->fstat(&st);
    if (err)
        return err;
    err = erofs_iostream_open_io_manager(&erofstar->ios,
                                         tar_io_manager, st.st_size);
    return err;
}

static int write_map_file(photon::fs::IFile *fout)
{
    FILE *fp;
    uint64_t blkaddr, toff;
    uint32_t nblocks;

    fp = fopen(MAP_FILE_NAME, "r");
    if (fp == NULL) {
       LOG_ERROR("unable to get upper.map, ignored");
       return -1;
    }

    while (fscanf(fp, "%" PRIx64" %x %" PRIx64 "\n", &blkaddr, &nblocks, &toff) >= 3) {
        LSMT::RemoteMapping lba;
        lba.offset = blkaddr * TAREROFS_BLOCK_SIZE;
        lba.count = nblocks * TAREROFS_BLOCK_SIZE;
        lba.roffset = toff;
        int nwrite = fout->ioctl(LSMT::IFileRW::RemoteData, lba);
        if ((unsigned) nwrite != lba.count) {
            LOG_ERRNO_RETURN(0, -1, "failed to write lba");
        }
    }

    fclose(fp);
    return 0;
}

int TarErofsInter::TarErofsImpl::extract_all() {
    struct erofs_sb_info sbi = {};
    struct erofs_tarfile erofstar = {};
    struct erofs_mkfs_cfg cfg;
    int err;

    /* initialization of sbi */    
    err = init_sbi(&sbi, &io_manager_img);
    if (err) {
        erofs_dev_close(&sbi);
        LOG_ERROR("Failed to init sbi.");
        return err;
    }
    /* initialization of erofstar */
    err = init_tarerofs(&erofstar, file, &tar_io_manager);
    if (err) {
        erofs_dev_close(&sbi);
        erofs_iostream_close(&erofstar.ios);
        LOG_ERROR("Failed to init tarerofs");
        return err;
    }

    cfg.sbi = &sbi;
    cfg.erofstar = &erofstar;
    cfg.index_mode = true;
    cfg.data_offset = DATA_OFFSET;
    cfg.append_mode = !first_layer;
    cfg.ovlfs_strip = true;
    cfg.base_io_manager = !first_layer ? &io_manager_base : NULL;

    err = erofs_mkfs(&cfg);
    if (err) {
        erofs_dev_close(&sbi);
        erofs_iostream_close(&erofstar.ios);
        LOG_ERROR("Failed to mkfs.");
        return err;
    }

    /* write mapfile */
    err = write_map_file(fout);
    if (err) {
        erofs_dev_close(&sbi);
        erofs_iostream_close(&erofstar.ios);
        LOG_ERROR("Failed to write mapfile.");
        return err;
    }

    erofs_dev_close(&sbi);
    erofs_iostream_close(&erofstar.ios);
    return 0;
}

TarErofsInter::TarErofsInter(photon::fs::IFile *file, photon::fs::IFile *target, uint64_t fs_blocksize,
          photon::fs::IFile *bf, bool meta_only, bool first_layer) :
          impl(new TarErofsImpl(file, target, fs_blocksize, bf, meta_only, first_layer))
{
}

TarErofsInter:: ~TarErofsInter()
{
    delete impl;
}

int TarErofsInter::extract_all()
{
    return impl->extract_all();
}