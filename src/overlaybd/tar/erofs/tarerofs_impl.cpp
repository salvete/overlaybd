#include "tarerofs_interface.h"
#include "tarerofs_impl.h"
#include "erofs/tarerofs_api.h"
#include "erofs/tar.h"
#include "erofs/io.h"
#include "../../lsmt/file.h"
#include "../../lsmt/index.h"
#include "../../../image_file.h"
#include <photon/common/alog.h>

#define TAREROFS_BLOCK_SIZE 4096
#define TAREROFS_BLOCK_BITS 12
#define DATA_OFFSET 1073741824
#define MIN_RW_LEN 512ULL
#define round_down_blk(addr) ((addr) & (~(MIN_RW_LEN - 1)))
#define round_up_blk(addr) (round_down_blk((addr) + MIN_RW_LEN - 1))
#define MAP_FILE_NAME "upper.map"

/* debug */
void write_to_file(const char *msg, ...)
{
        char *erofs_out_file = "/home/hongzhen/tarerofs_impl.txt";
        
        int buf_size = 100;
         char buffer[buf_size];
        // 打开文件，以追加模式。如果文件不存在，创建文件，权限为 0644（用户可读写，组和其他用户可读）
        int fd = open(erofs_out_file, O_WRONLY | O_APPEND | O_CREAT, 0644);
        if (fd == -1) {
                perror("Error opening file");
                return;
        }


        // 处理可变参数并生成格式化字符串
        va_list args;
        va_start(args, msg);
        vsnprintf(buffer, buf_size, msg, args);
        va_end(args);
        // 计算字符串的长度
        size_t len = strlen(buffer);

        buffer[len] = '\n';

        // 写入字符串到文件
        ssize_t written = write(fd, buffer, len);
        if (written == -1) {
                perror("Error writing to file");
                close(fd);
                return;
        } else if (written != (ssize_t)len) {
                fprintf(stderr, "Partial write error: wrote %zd of %zu bytes\n", written, len);
                close(fd);
                return;
        }

        // 关闭文件
        if (close(fd) == -1) {
                perror("Error closing file");
        }
}

/*
 * Helper function for reading from the photon file, since
 * the photon file requires reads to be 512-byte aligned.
 */
static ssize_t read_photon_file(void *buf, u64 offset, size_t len, photon::fs::IFile *file)
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
 static ssize_t write_photon_file(const void *buf, u64 offset, size_t len, photon::fs::IFile *file)
 {
     size_t ret;
     u64 start, end;
     size_t saved_len = len;
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
 
     return saved_len;
 }
 
TarErofsInter::TarErofsImpl *TarErofsInter::TarErofsImpl::ops_to_tarerofsimpl(struct erofs_vfops *ops)
{
    struct erofs_vfops_wrapper *evw = reinterpret_cast<struct erofs_vfops_wrapper*>(ops);
    TarErofsImpl *obj = reinterpret_cast<TarErofsImpl*>(evw->private_data);
    return obj;
}

/* I/O control for target */
ssize_t TarErofsInter::TarErofsImpl::target_pread(struct erofs_vfile *vf, void *buf, u64 offset, size_t len)
{
    TarErofsImpl *obj = ops_to_tarerofsimpl(vf->ops);
    photon::fs::IFile *fout = obj->fout;

    if (read_photon_file(buf, offset, len, fout) != (ssize_t)len)
        return -1;
    return len;
}

ssize_t TarErofsInter::TarErofsImpl::target_pwrite(struct erofs_vfile *vf, const void *buf, u64 offset, size_t len)
{
    TarErofsImpl *obj = ops_to_tarerofsimpl(vf->ops);
    photon::fs::IFile *fout = obj->fout;
    ssize_t ret;

    if (!buf)
        return -EINVAL;

    ret = write_photon_file(buf, offset, len, fout);
    return ret;
}

int TarErofsInter::TarErofsImpl::target_fsync(struct erofs_vfile *vf)
{
    TarErofsImpl *obj = ops_to_tarerofsimpl(vf->ops);
    photon::fs::IFile *fout = obj->fout;

    return fout->fsync();
}

int TarErofsInter::TarErofsImpl::target_fallocate(struct erofs_vfile *vf, u64 offset, size_t len, bool pad)
{
	static const char zero[4096] = {0};
	ssize_t ret;

	while (len > 4096) {
		ret = target_pwrite(vf, zero, offset, 4096);
		if (ret)
			return ret;
		len -= 4096;
		offset += 4096;
	}
	ret = target_pwrite(vf, zero, offset, len);
	if (ret != (ssize_t)len) {
		return -2;
	}
	return 0;
}

int TarErofsInter::TarErofsImpl::target_ftruncate(struct erofs_vfile *vf, u64 length)
{
    return 0;
}


ssize_t TarErofsInter::TarErofsImpl::target_read(struct erofs_vfile *vf, void *buf, size_t len)
{
    return -1;
}

off_t TarErofsInter::TarErofsImpl::target_lseek(struct erofs_vfile *vf, u64 offset, int whence)
{
    return -1;
}

/* I/O control for source */
ssize_t TarErofsInter::TarErofsImpl::source_pread(struct erofs_vfile *vf, void *buf, u64 offset, size_t len)
{
    return -1; 
}

ssize_t TarErofsInter::TarErofsImpl::source_pwrite(struct erofs_vfile *vf, const void *buf, u64 offset, size_t len)
{
    return -1;
}

int TarErofsInter::TarErofsImpl::source_fsync(struct erofs_vfile *vf)
{
    return -1;
}

int TarErofsInter::TarErofsImpl::source_fallocate(struct erofs_vfile *vf, u64 offset, size_t len, bool pad)
{
    return -1;
}

int TarErofsInter::TarErofsImpl::source_ftruncate(struct erofs_vfile *vf, u64 length)
{
    return -1;
}


ssize_t TarErofsInter::TarErofsImpl::source_read(struct erofs_vfile *vf, void *buf, size_t bytes)
{
    TarErofsImpl *obj = ops_to_tarerofsimpl(vf->ops);

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


off_t TarErofsInter::TarErofsImpl::source_lseek(struct erofs_vfile *vf, u64 offset, int whence)
{
    TarErofsImpl *obj = ops_to_tarerofsimpl(vf->ops);
    photon::fs::IFile *file = obj->file;

    return file->lseek(offset, whence);
}


/* I/O control for base */
ssize_t TarErofsInter::TarErofsImpl::base_pread(struct erofs_vfile *vf, void *buf, u64 offset, size_t len)
{
    TarErofsImpl *obj = ops_to_tarerofsimpl(vf->ops);
    photon::fs::IFile *fs_base_file = obj->fs_base_file;

    if (read_photon_file(buf, offset, len, fs_base_file) != (ssize_t)len)
        return -1;
    
    return len;
}

ssize_t TarErofsInter::TarErofsImpl::base_pwrite(struct erofs_vfile *vf, const void *buf, u64 offset, size_t len)
{
    return -1;
}

int TarErofsInter::TarErofsImpl::base_fsync(struct erofs_vfile *vf)
{
    return -1;
}

int TarErofsInter::TarErofsImpl::base_fallocate(struct erofs_vfile *vf, u64 offset, size_t len, bool pad)
{
    return -1;
}

int TarErofsInter::TarErofsImpl::base_ftruncate(struct erofs_vfile *vf, u64 length)
{
    return -1;
}


ssize_t TarErofsInter::TarErofsImpl::base_read(struct erofs_vfile *vf, void *buf, size_t len)
{
    return -1;
}


off_t TarErofsInter::TarErofsImpl::base_lseek(struct erofs_vfile *vf, u64 offset, int whence)
{
    return -1;
}

static int init_sbi(struct erofs_sb_info *sbi, photon::fs::IFile *fout, struct erofs_vfops *ops)
{
    int err;
    struct timeval t;

    sbi->blkszbits = TAREROFS_BLOCK_BITS;    
    err = gettimeofday(&t, NULL);
    if (err) 
        return err;
    sbi->build_time = t.tv_sec;
    sbi->build_time_nsec = t.tv_usec;
    sbi->bdev.ops = ops;
    fout->lseek(0, 0);
    sbi->devsz = INT64_MAX;

    return 0;
}

static int init_tar(struct erofs_tarfile *erofstar, photon::fs::IFile *tar_file, struct erofs_vfops *ops)
{
    int err;
    struct stat st;

    erofstar->global.xattrs = LIST_HEAD_INIT(erofstar->global.xattrs);
    erofstar->index_mode = true;
    erofstar->mapfile = MAP_FILE_NAME;
    erofstar->aufs = true;

    erofstar->ios.feof = false;
    erofstar->ios.tail = erofstar->ios.head = 0;
    erofstar->ios.dumpfd = -1;
    err = tar_file->fstat(&st);
    if (err)
        return err;
    erofstar->ios.sz = st.st_size;
    erofstar->ios.bufsize = 16384;
    do {
            erofstar->ios.buffer = (char*)malloc(erofstar->ios.bufsize);
            if (erofstar->ios.buffer)
                    break;
            erofstar->ios.bufsize >>= 1;
    } while (erofstar->ios.bufsize >= 1024);

    if (!erofstar->ios.buffer)
            return -ENOMEM;

    erofstar->ios.vf.ops = ops;

    return 0;
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

static void close_sbi(struct erofs_sb_info *sbi)
{
    return;
}

static void close_tar(struct erofs_tarfile *erofstar)
{
    if (erofstar->base)
        free(erofstar->base);
    free(erofstar->ios.buffer);
}

int TarErofsInter::TarErofsImpl::extract_all() {
    struct erofs_sb_info sbi = {};
    struct erofs_tarfile erofstar = {};
    struct erofs_mkfs_cfg cfg;
    int err;

    /* initialization of sbi */    
    err = init_sbi(&sbi, fout, reinterpret_cast<struct erofs_vfops*>(&target_vfops));
    if (err) {
        close_sbi(&sbi);
        LOG_ERROR("Failed to init sbi.");
        return err;
    }
    /* initialization of erofstar */
    err = init_tar(&erofstar, file, reinterpret_cast<struct erofs_vfops*>(&source_vfops));
    if (err) {
        close_sbi(&sbi);
        close_tar(&erofstar);
        LOG_ERROR("Failed to init tarerofs");
        return err;
    }

    if (!first_layer) {
        erofstar.base = (struct erofs_vfile*)malloc(sizeof(struct erofs_vfile));
        if (!erofstar.base) {
            LOG_ERROR("Failed to malloc erofstar.base");
            return -ENOMEM;
        }
        erofstar.base->ops = reinterpret_cast<struct erofs_vfops*>(&base_vfops);
    }

    cfg.sbi = &sbi;
    cfg.erofstar = &erofstar;
    cfg.data_offset = DATA_OFFSET;
    cfg.append_mode = !first_layer;
    cfg.ovlfs_strip = true;

    err = erofs_mkfs(&cfg);
    if (err) {
        close_sbi(&sbi);
        close_tar(&erofstar);
        LOG_ERROR("Failed to mkfs.");
        return err;
    }

    /* write mapfile */
    err = write_map_file(fout);
    if (err) {
        close_sbi(&sbi);
        close_tar(&erofstar);
        LOG_ERROR("Failed to write mapfile.");
        return err;
    }

    close_sbi(&sbi);
    close_tar(&erofstar);
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
