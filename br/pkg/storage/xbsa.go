package storage

import (
	"bytes"
	"context"
	"net/url"
	"path/filepath"

	"github.com/pingcap/errors"
	berrors "github.com/pingcap/tidb/br/pkg/errors"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// var bsaHandle int64 = -1

type XBSAStorage struct {
	remote string
}

// Convert `xbsa:///path` to `/path`
func tryConvertToPath(u string) (string, error) {
    parsedUrl, err := url.Parse(u)
    if err != nil {
        return "", err
    }

    if parsedUrl.Host == "" {
        return parsedUrl.Path, nil
    } else {
        return parsedUrl.String(), nil
    }
}

func (x *XBSAStorage) DeleteFile(_ context.Context, name string) error {
	log.Info("-----------DeleteFile", zap.String("Filename", name))
	// return nil
	return errors.Annotatef(berrors.ErrUnsupportedOperation, "DeleteFile hasn't been implemented!")
}

func (x *XBSAStorage) WriteFile(_ context.Context, name string, data []byte) error {
	log.Info("-----------WriteFile: ", zap.String("Filename", name), zap.String("remote", x.remote))

	var bsaHandle int64 = -1
	const CHAR_STRING_BUFFER_SIZE = 100
	env := make([][]byte, 4)
	env[0] = make([]byte, CHAR_STRING_BUFFER_SIZE)
	copy(env[0], []byte("BSA_API_VERSION=1.1.0"))
	env[1] = make([]byte, CHAR_STRING_BUFFER_SIZE)
	copy(env[1], []byte("BSA_SERVICE_HOST=http://192.168.144.143:50305/"))
	env[2] = make([]byte, CHAR_STRING_BUFFER_SIZE)
	copy(env[2], []byte("BSA_POLICY=0c04b35ecf9711ed8000000c296f9596"))
	env[3] = nil
	log.Info("--BSAInit", zap.Int("return", int(BSAInit(&bsaHandle, nil, nil, env))),
						  zap.Int64("bsaHandle", bsaHandle))
	log.Info("---BSABeginTxn", zap.Int("return", int(BSABeginTxn(bsaHandle))))

	p, _ := tryConvertToPath(x.remote)
	log.Info("-----------p: ", zap.String("p", p), zap.String("remote", x.remote))
	objName := ObjectName{
		objectSpaceName: []byte("obj"),
		pathName: []byte(filepath.Join(p, name)),
	}
	objDesc := ObjectDescriptor{objName, ObjectInfo{}}
	log.Info("----BSACreateObject", zap.Int("return", int(BSACreateObject(bsaHandle, &objDesc, nil))))

	blk := &DataBlock{bufferLen:   uint(len(data)),
					  numBytes:    uint(len(data)),
					  headerBytes: 0,
					  shareId:     0,
					  shareOffset: 0,
					  bufferPtr:   data,}
	log.Info("--BSASendData", zap.Int("return", int(BSASendData(bsaHandle, blk))))
	log.Info("--BSAEndData", zap.Int("return", int(BSAEndData(bsaHandle))))

	return nil
}

func (x *XBSAStorage) ReadFile(_ context.Context, name string) ([]byte, error) {
	log.Info("-----------ReadFile", zap.String("Filename", name))

	var bsaHandle int64 = -1
	const CHAR_STRING_BUFFER_SIZE = 100
	env := make([][]byte, 4)
	env[0] = make([]byte, CHAR_STRING_BUFFER_SIZE)
	copy(env[0], []byte("BSA_API_VERSION=1.1.0"))
	env[1] = make([]byte, CHAR_STRING_BUFFER_SIZE)
	copy(env[1], []byte("BSA_SERVICE_HOST=http://192.168.144.143:50305/"))
	env[2] = make([]byte, CHAR_STRING_BUFFER_SIZE)
	copy(env[2], []byte("BSA_POLICY=0c04b35ecf9711ed8000000c296f9596"))
	env[3] = nil
	log.Info("--BSAInit", zap.Int("return", int(BSAInit(&bsaHandle, nil, nil, env))),
						  zap.Int64("bsaHandle", bsaHandle))
	log.Info("---BSABeginTxn", zap.Int("return", int(BSABeginTxn(bsaHandle))))

	p, _ := tryConvertToPath(x.remote)
	objName := ObjectName{
		objectSpaceName: []byte("obj"),
		pathName: []byte(filepath.Join(p, name)),
	}
	qd := QueryDescriptor{objName: objName}
	od := ObjectDescriptor{objName: objName}
	log.Info("---BSAQueryObject", zap.Int("return", int(BSAQueryObject(bsaHandle, &qd, &od))))
	od = ObjectDescriptor{objName: objName}
	recv := DataBlock{bufferPtr: make([]byte, 1024)}
	log.Info("---BSAGetObject", zap.Int("return", int(BSAGetObject(bsaHandle, &od, &recv))))
	recv.bufferPtr = []byte("siuuuuuuuu")
	buf := make([]byte, 0)
	for ret := int(BSAGetData(bsaHandle, &recv));; ret = int(BSAGetData(bsaHandle, &recv)){
		log.Info("---BSAGetData", zap.Int("return", ret))
		log.Info("---BSAGetData", zap.ByteString("recv", recv.bufferPtr))
		
		if ret == BSA_RC_NO_MORE_DATA {
			buf = append(buf, recv.bufferPtr[:bytes.IndexByte(recv.bufferPtr, 0)]...)
			break
		}
		buf = append(buf, recv.bufferPtr...)
	}
	return buf, nil
}

func (x *XBSAStorage) FileExists(_ context.Context, name string) (bool, error) {
	log.Info("-----------FileExists", zap.String("Filename", name))
	// apiV := &ApiVersion{version:10, release:10, level:10}
	// log.Info("ApiVersion",
	// 		zap.Int("return", int(BSAQueryApiVersion(apiV))),
	// 		zap.Uint16("version", uint16(apiV.version)),
	// 		zap.Uint16("release", uint16(apiV.release)),
	// 		zap.Uint16("level", uint16(apiV.level)))

	var bsaHandle int64 = -1
	const CHAR_STRING_BUFFER_SIZE = 100
	env := make([][]byte, 4)
	env[0] = make([]byte, CHAR_STRING_BUFFER_SIZE)
	copy(env[0], []byte("BSA_API_VERSION=1.1.0"))
	env[1] = make([]byte, CHAR_STRING_BUFFER_SIZE)
	copy(env[1], []byte("BSA_SERVICE_HOST=http://192.168.144.143:50305/"))
	env[2] = make([]byte, CHAR_STRING_BUFFER_SIZE)
	copy(env[2], []byte("BSA_POLICY=0c04b35ecf9711ed8000000c296f9596"))
	// env[3] = new(string)
	// *env[3] = fmt.Sprintf("BSA_BACKUP_UUID=%s", backup_uuid)
	// env[4] = new(string)
	// *env[4] = "BSA_DB_VERSION=TBase V0.0.0"
	env[3] = nil
	// var ptrs []*byte
	// for _, s := range env {
	// 	if s == nil {
	// 		ptrs = append(ptrs, nil)
	// 	} else {
	// 		ptr := &s[0]
	// 		ptrs = append(ptrs, ptr)
	// 	}
	// }
	log.Info("--BSAInit", zap.Int("return", int(BSAInit(&bsaHandle, nil, nil, env))),
								zap.Int64("bsaHandle", bsaHandle))
	BSABeginTxn(bsaHandle)
	p, _ := tryConvertToPath(x.remote)
	objName := ObjectName{
		objectSpaceName: []byte("obj"),
		pathName: []byte(filepath.Join(p, name)),
	}
	qd := QueryDescriptor{objName: objName}
	od := ObjectDescriptor{}
	ret := int(BSAQueryObject(bsaHandle, &qd, &od))
	log.Info("---BSAQueryObject", zap.String("return", BSA_RC[ret]))
	if ret != BSA_RC_SUCCESS {
		return false, nil
	}
	return true, nil
}

func (x *XBSAStorage) WalkDir(_ context.Context, opt *WalkOption, fn func(string, int64) error) error {
	log.Info("-----------WalkDir")
	// return nil
	return errors.Annotatef(berrors.ErrUnsupportedOperation, "WalkDir hasn't been implemented!")
}

func (x *XBSAStorage) URI() string {
	log.Info("-----------URI")
	return x.remote
}

func (x *XBSAStorage) Open(_ context.Context, path string) (ExternalFileReader, error) {
	log.Info("-----------Open", zap.String("Pathname", path))
	// return nil, nil
	return nil, errors.Annotatef(berrors.ErrUnsupportedOperation, "Open hasn't been implemented!")
}

func (x *XBSAStorage) Create(_ context.Context, name string) (ExternalFileWriter, error) {
	log.Info("-----------Create", zap.String("Filename", name))
	// return nil, nil
	return nil, errors.Annotatef(berrors.ErrUnsupportedOperation, "Create hasn't been implemented!")
}

func (x *XBSAStorage) Rename(_ context.Context, oldFileName, newFileName string) error {
	log.Info("-----------Rename")
	// return nil
	return errors.Annotatef(berrors.ErrUnsupportedOperation, "Rename hasn't been implemented!")
}

func NewXBSAStorage(remote string) (*XBSAStorage, error) {
	log.Info("-----------remote: ", zap.String("remote", remote))
	return &XBSAStorage{remote: remote}, nil
}