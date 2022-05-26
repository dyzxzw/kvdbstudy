package minidb

import "os"

const FileName = "minidb.data"
const MergeFileName = "minidb.data.merge"

// DBFile 数据文件定义
type DBFile struct {
	File   *os.File  //文件指针
	Offset int64  //文件大小
}
//O_CREATE：文件不存在就创建
//os.O_RDWR:读写模式
func newInternal(fileName string) (*DBFile, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	/*
	os.Stat(fileName) 返回文件信息：
		        FileInfo：文件信息
			    interface
				Name()，文件名
				Size()，文件大小，字节为单位
				IsDir()，是否是目录
				ModTime()，修改时间
				Mode()，权限
	*/
	stat, err := os.Stat(fileName)
	if err != nil {
		return nil, err
	}
	//
	return &DBFile{Offset: stat.Size(), File: file}, nil
}

// NewDBFile 创建一个新的数据文件
func NewDBFile(path string) (*DBFile, error) {
	// os.PathSeparator路径分隔符（分隔路径元素）
	// path/minidb.data
	fileName := path + string(os.PathSeparator) + FileName
	return newInternal(fileName)
}

// NewMergeDBFile 新建一个合并时的数据文件
//os.PathSeparator 路径分隔符（分隔路径元素）
func NewMergeDBFile(path string) (*DBFile, error) {
	fileName := path + string(os.PathSeparator) + MergeFileName
	return newInternal(fileName)
}

// Read 从 offset 处开始读取
func (df *DBFile) Read(offset int64) (e *Entry, err error) {
	buf := make([]byte, entryHeaderSize)
	if _, err = df.File.ReadAt(buf, offset); err != nil {
		return
	}
	if e, err = Decode(buf); err != nil {
		return
	}

	offset += entryHeaderSize
	if e.KeySize > 0 {
		key := make([]byte, e.KeySize)
		if _, err = df.File.ReadAt(key, offset); err != nil {
			return
		}
		e.Key = key
	}

	offset += int64(e.KeySize)
	if e.ValueSize > 0 {
		value := make([]byte, e.ValueSize)
		if _, err = df.File.ReadAt(value, offset); err != nil {
			return
		}
		e.Value = value
	}
	return
}

// Write 写入 Entry
func (df *DBFile) Write(e *Entry) (err error) {
	enc, err := e.Encode()
	if err != nil {
		return err
	}
	_, err = df.File.WriteAt(enc, df.Offset)
	df.Offset += e.GetSize()
	return
}
