#include <stdlib.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <string.h>
#include <linux/lustre/lustre_fid.h>
#include <lustre/lustreapi.h>
#include <libgen.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdlib.h>
#include <sys/mman.h>

typedef struct
{
	char *hsm_import_dir;
	struct stat *st_hsm_root;
	char *import_list;
	int import_size;
	sem_t *sem_p;
	pthread_t tid;
	int rc;
} lhsm_task_ctl;

static int hsm_root_st_mode(const char *hsm_import_dir, struct stat *st_hsm_root)
{
	int rc = stat(hsm_import_dir, st_hsm_root);
	if (rc)
	{
		printf("cannot run stat on hsm import path '%s'", hsm_import_dir);
	}
	else
	{
		// set st_size to 0, all of import file will use same attribute from import
		// root directoy except file size
		st_hsm_root->st_size = 0;
	}
	return rc;
}

/**
 * Import a file from hsm backend into Lustre.
 *
 *
 * \param full_path      path to Lustre destination (e.g. /mnt/lustre/my/file).
 * \param src_nm         file name (object name) of HSM backend
 * \param st_dir         struct stat buffer containing lustre import root directory
 */
inline static int hsm_import_one(const char *full_path, const char *src_nm, const struct stat *st_dir)
{
	struct lu_fid  fid;
	int rc;

	rc = llapi_hsm_import(full_path,
						  1,
						  st_dir,
						  0 /* default stripe_size */,
						  -1 /* default stripe offset */,
						  0 /* default stripe count */,
						  0 /* stripe pattern (will be RAID0+RELEASED) */,
						  NULL /* pool_name */,
						  &fid);

	if (rc < 0)
	{
		int rc1 = access(full_path, F_OK);
		// skip exist file
		if (rc1 != -1)
		{
			printf("already import '%s' from '%s'", full_path, src_nm);
			return 0;
		}
	}
	return rc;
}

void * lhsm_import_one_batch(void * arg)
{
	lhsm_task_ctl *task_ctl_p = (lhsm_task_ctl *)arg;
	char *import_ptr = task_ctl_p->import_list;
	char full_path[PATH_MAX];

	for (int i = 0; i < task_ctl_p->import_size; i++)
	{
		sprintf(full_path, "%s/%s", task_ctl_p->hsm_import_dir, import_ptr);
		int rc = hsm_import_one(full_path, import_ptr, task_ctl_p->st_hsm_root);
		if (!rc)
		{
			import_ptr += PATH_MAX;
		}
		else
		{
			task_ctl_p->rc = rc;
			break;
		}
	}

	sem_post(task_ctl_p->sem_p);
	return NULL;
}

int main(int argc, char **argv)
{
	int rc = 0;
	char cmd_name[PATH_MAX];

	if (argc != 4)
	{
		printf("'%s import_path list_file batch_size'", cmd_name);
		return ENOEXEC;
	}

	struct stat st_hsm_root;
	char *hsm_import_root = argv[1];
	char *hsm_import_list = argv[2];
	int batch_size = atol(argv[3]);
	lhsm_task_ctl batch_ctl[256];
	int batch_idx = 0;
	rc = hsm_root_st_mode(hsm_import_root, &st_hsm_root);
	if (rc)
	{
		printf("failed to access import directory '%s'", hsm_import_root);
		return rc;
	}

	int fd;
	fd = open(hsm_import_list, O_RDONLY);
	if (fd == -1)
	{
		rc = EIO;
		printf("failed to access import list file '%s'", hsm_import_list);
		return rc;
	}

	struct stat st_list;
	if (fstat(fd, &st_list) == -1)
	{
		rc = EIO;
		printf("failed to stat import list file '%s'", hsm_import_list);
		close(fd);
		return rc;
	}

	char *addr;
	addr = mmap(NULL, st_list.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
	if (addr == MAP_FAILED)
	{
		rc = EIO;
		printf("failed to mmap import list file '%s'", hsm_import_list);
		close(fd);
		return rc;
	}

	char line_buf[PATH_MAX], full_path[PATH_MAX];
	int task_size;
	char *nl_ptr = NULL;
	char *cur_ptr = addr;
	bool quit_loop = false;
	bool next_loop = true;
	sem_t sem_task;
	sem_init(&sem_task, 0, 0);
	for (;;)
	{
		batch_ctl[batch_idx].import_list = (char *)malloc(PATH_MAX * batch_size);
		if (batch_ctl[batch_idx].import_list == NULL)
		{
			printf("failed to allocate memory for batch import task");
			return ENOMEM;
		}
		batch_ctl[batch_idx].hsm_import_dir = hsm_import_root;
		batch_ctl[batch_idx].st_hsm_root = &st_list;
		batch_ctl[batch_idx].sem_p = &sem_task;
		batch_ctl[batch_idx].rc = 0;
		char *cur_dst = batch_ctl[batch_idx].import_list;
		next_loop = false;
		while (true)
		{
			nl_ptr = strchr(cur_ptr, '\n');
			if (nl_ptr == NULL)
			{
				// no item to proces any more
				quit_loop = true;
			}
			else
			{
				size_t item_sz = nl_ptr - cur_ptr;
				strncpy(cur_dst, cur_ptr, item_sz);
				cur_dst += PATH_MAX;
				cur_ptr = (nl_ptr + 1);
				batch_ctl[batch_idx].import_size++;
			}
			if (batch_ctl[batch_idx].import_size == batch_size || quit_loop)
			{
				// create thread
				int trc = pthread_create(&batch_ctl[batch_idx].tid,
										 NULL,
										 lhsm_import_one_batch,
										 (void *)&batch_ctl[batch_idx]);
				if (trc != 0)
				{
					quit_loop = true;
					break;
				}
				if ((!quit_loop || batch_ctl[batch_idx].import_size == batch_size) && !trc)
				{
					batch_idx++;
					next_loop = true;
				}
			}
			// no item to process
			if (quit_loop || next_loop)
				break;
		}
		if (quit_loop)
			break;
	}

task_done:

	if (MAP_FAILED != addr)
	{
		munmap(addr, st_list.st_size);
	}
	close(fd);

	if (batch_idx)
	{
		sem_wait(&sem_task);

		for (int i = 0; i < batch_idx; i++)
		{
			int rc1 = pthread_join(batch_ctl[i].tid, NULL);
			if (rc1)
			{
				printf("failed to join thread '%d'", batch_ctl[i].tid);
				rc++;
			}
			free(batch_ctl[i].import_list);
		}
	}

	// success import content given by list file, delete list file
	// we only need keep list file when failed to import, because it need retry
	// later by recovery process
	if (!rc)
	{
		if (remove(hsm_import_list))
		{
			printf("failed to delete import list file '%s'", hsm_import_list);
		}
	}

	sem_destroy(&sem_task);

	return rc;
}
