#!/usr/bin/env python

# Script implements attachments virtual filesystem for jira/confluence dev servers
#       in order to don't store attachments on dev servers (as it takes a lot of 
#       pace). File systemd downloads attachment files which required by jira only
# Check jcsfs.yaml for additional info
# Author: dmitrenok@netcracker.com

import os
import sys
import yaml
import logging
import argparse
import paramiko
from errno import ENOENT
from fuse import FUSE, LoggingMixIn, Operations, FuseOSError

VERSION = "0.1.1-dev"

class JCSFs(LoggingMixIn, Operations):
    def __getLocalPath(self, path):
        return self.__getPath(path, self.config['datadir'])

    def __getPath(self, path, root=None):
        if root is None:
            root = self.config['ssh'].get("path", "")
        return os.path.join(root, path.lstrip("/"))

    def __getConfig(self, cfg_fname):
        config = None
        with open(cfg_fname, "r") as f_cfg:
            config = yaml.safe_load(f_cfg)
        if "datadir" not in config:
            raise Exception("Datadir not hasn't been found in config")
        if not os.path.exists(config['datadir']):
            os.makedirs(config['datadir'])
        return config

    def __getSFTPClient(self):
        if self.sftp is not None:
            return self.sftp
    
        self.client.connect(
            self.config['ssh'].get("host"),
            port=int(self.config['ssh'].get("port", 22)),
            username=self.config['ssh'].get("login"),
            key_filename=self.config['ssh'].get("key")
        )
        self.sftp = self.client.open_sftp()
        return self.sftp

    def __downloadFileIfNotExists(self, path):
        local_path = self.__getLocalPath(path)
        if os.path.exists(local_path): return

        remote_path = self.__getPath(path)
        local_dir_name = os.path.dirname(local_path)
        if not os.path.exists(local_dir_name):
            os.makedirs(local_dir_name)
        sftp = self.__getSFTPClient()
        sftp.put(remote_path, local_path)

    def __init__(self, cfg_fname):
        self.config = self.__getConfig(cfg_fname)
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.sftp = None

    def destroy(self, path):
        self.sftp.close()
        self.client.close()

    def getattr(self, path, fh=None):
        if os.path.exists(self.__getLocalPath(path)):
            st = os.lstat(self.__getLocalPath(path))
        else:
            try:
                sftp = self.__getSFTPClient()
                st = sftp.lstat(self.__getPath(path))
            except IOError:
                raise FuseOSError(ENOENT)
        return { key:getattr(st, key) for key in 
            ('st_atime', 'st_gid', 'st_mode', 'st_mtime', 'st_size', 'st_uid')
        }

    def readdir(self, path, fh):
        files = ['.', '..']
        local_path = self.__getLocalPath(path)
        if os.path.exists(local_path):
            files = files + os.listdir(local_path)
        sftp = self.__getSFTPClient()
        return list(set(files + sftp.listdir(self.__getPath(path))))

    def mkdir(self, path, mode):
        os.mkdir(self.__getLocalPath(path))
        
    def read(self, path, size, offset, fh):
        self.__downloadFileIfNotExists(path)
        buf = None
        with open(self.__getLocalPath(path), "r+b") as f:
            f.seek(offset, 0)
            buf = f.read(size)
        return buf

    def write(self, path, data, offset, fh):
        self.__downloadFileIfNotExists(path)
        with open(self.__getLocalPath(path), "a+b") as f:
            f.seek(offset, 0)
            f.write(data)
        return len(data)
        
    def rename(self, old, new):
        self.__downloadFileIfNotExists(old)
        old_path = self.__getLocalPath(old)
        new_path = self.__getLocalPath(new)
        return os.rename(old_path, new_path)
        
    def rmdir(self, path):
        if os.path.exists(self.__getLocalPath(path)):
            os.rmdir(self.__getLocalPath(path))

    def unlink(self, path):
        os.unlink(self.__getLocalPath(path))

    def chmod(self, path, mode):
        self.__downloadFileIfNotExists(path)
        return os.chmod(self.__getLocalPath(path), mode)

    def chown(self, path, uid, gid):
        self.__downloadFileIfNotExists(path)
        return os.chown(self.__getLocalPath(path), uid, gid)

def main(argv):
    logging.basicConfig(level=logging.DEBUG, filename="/tmp/fuse.log") # TODO: should be replaced with parameter from config
    arg_parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    arg_parser.add_argument("cfg")
    arg_parser.add_argument("mountpoint")
    args = arg_parser.parse_args(argv)
    
    jcsfs = JCSFs(args.cfg)
    fuse = FUSE (
        jcsfs,
        args.mountpoint,
        nonempty=True,
        foreground=False,
        allow_other=True,
        nothreads=False,
        debug=False
    )

if __name__ == '__main__':
    main(sys.argv[1:])

