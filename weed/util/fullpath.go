package util

import (
	"path/filepath"
	"strings"
)

type FullPath string

func NewFullPath(dir, name string) FullPath {
	return FullPath(dir).Child(name)
}

func (fp FullPath) DirAndName() (string, string) {
	dir, name := filepath.Split(string(fp))
	name = strings.ToValidUTF8(name, "?")
	if dir == "/" {
		return dir, name
	}
	if len(dir) < 1 {
		return "/", ""
	}
	return dir[:len(dir)-1], name
}

func (fp FullPath) Name() string {
	_, name := filepath.Split(string(fp))
	name = strings.ToValidUTF8(name, "?")
	return name
}

func (fp FullPath) IsRootNode() bool {
	splits := strings.Split(strings.TrimLeft(string(fp), string(filepath.Separator)), string(filepath.Separator))
	return len(splits) == 1
}

func (fp FullPath) IsQuotaRootNode() bool {
	splits := strings.Split(strings.TrimLeft(string(fp), string(filepath.Separator)), string(filepath.Separator))
	return len(splits) == 1 && strings.Contains(splits[0], QuotaDirectoryPrefix)
}

const QuotaDirectoryPrefix = "quota-"

func (fp FullPath) GetRootDir() (exist bool, rootPath FullPath) {
	exist = strings.Contains(string(fp), QuotaDirectoryPrefix)
	if !exist {
		return
	}
	splits := strings.Split(strings.TrimLeft(string(fp), string(filepath.Separator)), string(filepath.Separator))
	// [quota-1, example-dir] 代表有root dir
	// [quota-1] 已经是root dir本身了，返回不存在即可
	if len(splits) < 2 {
		exist = false
		return
	}

	rootPath = FullPath(filepath.Separator)
	if len(splits) != 0 {
		rootPath += FullPath(splits[0])
	}

	return
}

func (fp FullPath) Child(name string) FullPath {
	dir := string(fp)
	noPrefix := name
	if strings.HasPrefix(name, "/") {
		noPrefix = name[1:]
	}
	if strings.HasSuffix(dir, "/") {
		return FullPath(dir + noPrefix)
	}
	return FullPath(dir + "/" + noPrefix)
}

// AsInode an in-memory only inode representation
func (fp FullPath) AsInode(unixTime int64) uint64 {
	inode := uint64(HashStringToLong(string(fp)))
	inode = inode + uint64(unixTime)*37
	return inode
}

// split, but skipping the root
func (fp FullPath) Split() []string {
	if fp == "" || fp == "/" {
		return []string{}
	}
	return strings.Split(string(fp)[1:], "/")
}

func Join(names ...string) string {
	return filepath.ToSlash(filepath.Join(names...))
}

func JoinPath(names ...string) FullPath {
	return FullPath(Join(names...))
}

func (fp FullPath) IsUnder(other FullPath) bool {
	if other == "/" {
		return true
	}
	return strings.HasPrefix(string(fp), string(other)+"/")
}

func StringSplit(separatedValues string, sep string) []string {
	if separatedValues == "" {
		return nil
	}
	return strings.Split(separatedValues, sep)
}
