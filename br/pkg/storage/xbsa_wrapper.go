package storage

/*
// #cgo LDFLAGS: -L/home/tidb/xbsa_so/go/ -ltest
// #cgo LDFLAGS: -L/home/tidb/xbsa_so/scutech_xbsa_so/ -lxbsa64
// #cgo LDFLAGS: -L/home/tidb/xbsa_so/scutech_xbsa_so/ -lxbsa64i
#cgo LDFLAGS: -L/opt/scutech/dbackup3/lib/ -lxbsa64

// #include "/home/tidb/xbsa_so/xbsa/xbsa.h"
#include "/home/tidb/xbsa_so/scutech_xbsa_so/xbsa.h"
*/
import "C"

import (
	"unsafe"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// const BSA_MAX_TOKEN_SIZE int = int(C.BSA_MAX_TOKEN_SIZE)
// const BSA_MAX_BSAOBJECT_OWNER int = int(C.BSA_MAX_BSAOBJECT_OWNER)
// const BSA_MAX_APPOBJECT_OWNER int = int(C.BSA_MAX_APPOBJECT_OWNER)


const (
    BSA_RC_SUCCESS                  int = 0x00
    BSA_RC_ABORT_SYSTEM_ERROR       int = 0x03
    BSA_RC_ACCESS_FAILURE           int = 0x4D
    BSA_RC_AUTHENTICATION_FAILURE   int = 0x04
    BSA_RC_BUFFER_TOO_SMALL         int = 0x4E
    BSA_RC_INVALID_CALL_SEQUENCE    int = 0x05
    BSA_RC_INVALID_COPYID           int = 0x4F
    BSA_RC_INVALID_DATABLOCK        int = 0x34
    BSA_RC_INVALID_ENV              int = 0x50
    BSA_RC_INVALID_HANDLE           int = 0x06
    BSA_RC_INVALID_OBJECTDESCRIPTOR int = 0x51
    BSA_RC_INVALID_QUERYDESCRIPTOR  int = 0x53
    BSA_RC_INVALID_VOTE             int = 0x0B
    BSA_RC_NO_MATCH                 int = 0x11
    BSA_RC_NO_MORE_DATA             int = 0x12
    BSA_RC_NULL_ARGUMENT            int = 0x55
    BSA_RC_OBJECT_NOT_FOUND         int = 0x1A
    BSA_RC_TRANSACTION_ABORTED      int = 0x20
    BSA_RC_VERSION_NOT_SUPPORTED    int = 0x4B
)
// use for error string
var BSA_RC = map[int]string{
    0x00: "BSA_RC_SUCCESS",
    0x03: "BSA_RC_ABORT_SYSTEM_ERROR",
    0x4D: "BSA_RC_ACCESS_FAILURE",
    0x04: "BSA_RC_AUTHENTICATION_FAILURE",
    0x4E: "BSA_RC_BUFFER_TOO_SMALL",
    0x05: "BSA_RC_INVALID_CALL_SEQUENCE",
    0x4F: "BSA_RC_INVALID_COPYID",
    0x34: "BSA_RC_INVALID_DATABLOCK",
    0x50: "BSA_RC_INVALID_ENV",
    0x06: "BSA_RC_INVALID_HANDLE",
    0x51: "BSA_RC_INVALID_OBJECTDESCRIPTOR",
    0x53: "BSA_RC_INVALID_QUERYDESCRIPTOR",
    0x0B: "BSA_RC_INVALID_VOTE",
    0x11: "BSA_RC_NO_MATCH",
    0x12: "BSA_RC_NO_MORE_DATA",
    0x55: "BSA_RC_NULL_ARGUMENT",
    0x1A: "BSA_RC_OBJECT_NOT_FOUND",
    0x20: "BSA_RC_TRANSACTION_ABORTED",
    0x4B: "BSA_RC_VERSION_NOT_SUPPORTED",
}


type BSA_Int32       int
type BSA_ShareId     int
type BSA_UInt32      uint
type BSA_UInt16      uint16
type Handle_Index    int64
type SecurityToken   [C.BSA_MAX_TOKEN_SIZE]byte
type bsa_ObjectOwner [C.BSA_MAX_BSAOBJECT_OWNER]byte
type app_ObjectOwner [C.BSA_MAX_APPOBJECT_OWNER]byte
type ObjectInfo      [C.BSA_MAX_OBJINFO]byte

type ApiVersion struct {
    version   BSA_UInt16
    release   BSA_UInt16
    level     BSA_UInt16
}
type ObjectOwner struct {
    bsaObjectOwner bsa_ObjectOwner
    appObjectOwner app_ObjectOwner
}
type ObjectName struct {
    // objectSpaceName [C.BSA_MAX_OSNAME]byte
    objectSpaceName []byte
    // pathName [C.BSA_MAX_PATHNAME]byte
    pathName []byte
}
type ObjectName2 struct {
    objectSpaceName  [C.BSA_MAX_OSNAME]C.char
    pathName         [C.BSA_MAX_PATHNAME]C.char
}

type ObjectDescriptor struct {
    // version         BSA_UInt32      // Version number for this structure
    // owner           ObjectOwner     // Owner of the object
    objName         ObjectName      // Object name
    // createTime      time.Time       // Supplied by Backup Services
    // copyType        CopyType        // Copy type: archive or backup
    // copyId          CopyId          // Supplied by Backup Services
    // restoreOrder    BSA_UInt64      // Supplied by Backup Services
    // lGName          LGName          // Associated Lifecycle Group name
    // cGName          CopyGpName      // Copy group within the lifecycle group
    // size            ObjectSize      // Object size may be up to 63 bits
    // resourceType    ResourceType    // e.g. UNIX file system
    // objectType      ObjectType      // e.g. file, directory, etc.
    // status          ObjectStatus    // Active/inactive, supplied by Backup Services
    // encodingList    EncodingMethod  // List of encoding Methods used, in application-defined order, terminated with a null entry
    // desc            Description     // Descriptive label for the object
    objectInfo      ObjectInfo      // Application information
}
type ObjectDescriptor2 struct {
    objName     C.ObjectName
    objectInfo  C.ObjectInfo
}
type QueryDescriptor struct {
    // owner         ObjectOwner     // Owner of the object                    
    objName       ObjectName      // Object name                            
    // createTimeLB  time.Time       // Lower bound on create time             
    // createTimeUB  time.Time       // Upper bound on create time             
    // expireTimeLB  time.Time       // Lower bound on expiration time         
    // expireTimeUB  time.Time       // Upper bound on expiration time         
    // copyType      CopyType        // Copy type: archive or backup           
    // lGName        LGName          // Associated Lifecycle Group name        
    // cGName        CopyGpName      // Copy group within the lifecycle group  
    // resourceType  ResourceType    // e.g. UNIX file system                  
    // objectType    ObjectType      // e.g. file, directory, etc.             
    // status        ObjectStatus    // Active/inactive, supplied by Backup    
    // desc          QueryDescription// Descriptive label for the object       
}
type QueryDescriptor2 struct {
    objName     C.ObjectName
}

type DataBlock struct {
    bufferLen   uint
    numBytes    uint
    headerBytes uint
    shareId     int
    shareOffset uint
    bufferPtr   []byte
}
type DataBlock2 struct {
    bufferLen   C.BSA_UInt32
    numBytes    C.BSA_UInt32
    headerBytes C.BSA_UInt32
    shareId     C.BSA_ShareId
    shareOffset C.BSA_UInt32
    bufferPtr   unsafe.Pointer
}
type Vote int
const (
    BSA_Vote_COMMIT Vote = 0
    BSA_Vote_ABORT  Vote = 1
)

func convertObjectDescriptor(obj *ObjectDescriptor) *ObjectDescriptor2 {
    var c [C.BSA_MAX_OSNAME]C.char
    copy((*[C.BSA_MAX_OSNAME]byte)(unsafe.Pointer(&c[0]))[:], obj.objName.objectSpaceName)
    var d [C.BSA_MAX_PATHNAME]C.char
    copy((*[C.BSA_MAX_PATHNAME]byte)(unsafe.Pointer(&d[0]))[:], obj.objName.pathName)
    objName := ObjectName2{objectSpaceName: c,
                           pathName: d}
    return &ObjectDescriptor2{objName: *(*C.ObjectName)(unsafe.Pointer(&objName)), objectInfo: *(*C.ObjectInfo)(unsafe.Pointer(&obj.objectInfo[0]))}
}
func convertObjectDescriptor2(obj *ObjectDescriptor2) *ObjectDescriptor {
    // TODO
    return &ObjectDescriptor{}
}
func convertDataBlock(blk *DataBlock) *DataBlock2 {
    return &DataBlock2{bufferLen: C.BSA_UInt32(blk.bufferLen),
                       numBytes: C.BSA_UInt32(blk.numBytes),
                       headerBytes: C.BSA_UInt32(blk.headerBytes),
                       shareId: C.BSA_ShareId(blk.shareId),
                       shareOffset: C.BSA_UInt32(blk.shareOffset),
                       bufferPtr: unsafe.Pointer(&blk.bufferPtr[0])}
}
func convertDataBlock2(blk2 *DataBlock2) *DataBlock {
    ret := &DataBlock{bufferLen:   uint(blk2.bufferLen),
                      numBytes:    uint(blk2.numBytes),
                      headerBytes: uint(blk2.headerBytes),
                      shareId:     int(blk2.shareId),
                      shareOffset: uint(blk2.shareOffset),
                      bufferPtr:   C.GoBytes(blk2.bufferPtr, C.int(blk2.bufferLen))}
    log.Info("--convertDataBlock2", zap.Uint("bufferLen", ret.bufferLen),
                                    zap.Uint("numBytes", ret.numBytes))
    return ret
}
func convertQueryDescriptor(obj *QueryDescriptor) *QueryDescriptor2 {
    var c [C.BSA_MAX_OSNAME]C.char
    copy((*[C.BSA_MAX_OSNAME]byte)(unsafe.Pointer(&c[0]))[:], obj.objName.objectSpaceName)
    var d [C.BSA_MAX_PATHNAME]C.char
    copy((*[C.BSA_MAX_PATHNAME]byte)(unsafe.Pointer(&d[0]))[:], obj.objName.pathName)
    objName := ObjectName2{objectSpaceName: c,
                           pathName: d}
    return &QueryDescriptor2{objName: *(*C.ObjectName)(unsafe.Pointer(&objName))}
}
// func BSAInit(bsaHandlePtr *Handle_Index, tokenPtr *SecurityToken, objectOwnerPtr *ObjectOwner, environmentPtr **byte) BSA_Int32 {
//     return BSA_Int32(C.BSAInit((*C.Handle_Index)(unsafe.Pointer(bsaHandlePtr)),
//                                (*C.SecurityToken)(unsafe.Pointer(tokenPtr)),
//                                (*C.ObjectOwner)(unsafe.Pointer(objectOwnerPtr)),
//                                (**C.char)(unsafe.Pointer(environmentPtr))))
// }
func BSAInit(bsaHandlePtr *int64, tokenPtr *SecurityToken, objectOwnerPtr *ObjectOwner, environmentPtr [][]byte) BSA_Int32 {
    cStrings := make([](*C.char), 4)
    for i, s := range environmentPtr {
        if s == nil {
            cStrings[i] = (*C.char)(C.NULL)
        } else {
            cStrings[i] = (*C.char)(unsafe.Pointer(&s[0]))
        }
    }
    // tmp := (*C.char)(unsafe.Pointer(&environmentPtr[0][0]))
    // ttmp := (**C.char)(unsafe.Pointer(&tmp))
    return BSA_Int32(C.BSAInit((*C.Handle_Index)(unsafe.Pointer(bsaHandlePtr)),
                               (*C.SecurityToken)(unsafe.Pointer(tokenPtr)),
                               (*C.ObjectOwner)(unsafe.Pointer(objectOwnerPtr)),
                               (**C.char)(unsafe.Pointer(&cStrings[0]))))
                            //    (**C.char)(unsafe.Pointer(ttmp))))
}

func BSAQueryApiVersion(apiVersionPtr *ApiVersion) BSA_Int32 {
    return BSA_Int32(C.BSAQueryApiVersion((*C.ApiVersion)(unsafe.Pointer(apiVersionPtr))))
}

func BSAQueryObject(bsaHandle int64, queryDescriptorPtr *QueryDescriptor, objectDescriptorPtr *ObjectDescriptor) BSA_Int32 {
    ptr := convertQueryDescriptor(queryDescriptorPtr)
    qd := C.QueryDescriptor{objName: ptr.objName}
    od := ObjectDescriptor2{}
    ret := BSA_Int32(C.BSAQueryObject((C.Handle_Index)(bsaHandle),
                                      &qd,
                                      (*C.ObjectDescriptor)(unsafe.Pointer(&od))))
    *objectDescriptorPtr = *convertObjectDescriptor2(&od)
    return ret
}

func BSABeginTxn(bsaHandle int64) BSA_Int32 {
    return BSA_Int32(C.BSABeginTxn((C.Handle_Index)(bsaHandle)))
}

func BSACreateObject(bsaHandle int64, objectDescriptorPtr *ObjectDescriptor, dataBlockPtr *DataBlock) BSA_Int32 {
    ptr := convertObjectDescriptor(objectDescriptorPtr)
	log.Info("--BSACreateObject", zap.String("objectSpaceName", string(objectDescriptorPtr.objName.objectSpaceName)))
    od := C.ObjectDescriptor{objName: ptr.objName, objectInfo: ptr.objectInfo}
    // blkPtr := convertDataBlock(dataBlockPtr)
    return BSA_Int32(C.BSACreateObject((C.Handle_Index)(bsaHandle),
                                    //    (*C.ObjectDescriptor)(unsafe.Pointer(objectDescriptorPtr)),
                                    //    (*C.ObjectDescriptor)(unsafe.Pointer(ptr)),
                                       &od,
                                       (*C.DataBlock)(unsafe.Pointer(dataBlockPtr))))
}

func BSAGetObject(bsaHandle int64, objectDescriptorPtr *ObjectDescriptor, dataBlockPtr *DataBlock) BSA_Int32 {
    ptr := convertObjectDescriptor(objectDescriptorPtr)
    od := C.ObjectDescriptor{objName: ptr.objName, objectInfo: ptr.objectInfo}
    // blkPtr := convertDataBlock(dataBlockPtr)
    return BSA_Int32(C.BSAGetObject((C.Handle_Index)(bsaHandle),
                                     &od,
                                    (*C.DataBlock)(unsafe.Pointer(dataBlockPtr))))
}

func BSASendData(bsaHandle int64, dataBlockPtr *DataBlock) BSA_Int32 {
    blk2Ptr := convertDataBlock(dataBlockPtr)
    return BSA_Int32(C.BSASendData((C.Handle_Index)(bsaHandle),
                                   (*C.DataBlock)(unsafe.Pointer(blk2Ptr))))
}

func BSAGetData(bsaHandle int64, dataBlockPtr *DataBlock) BSA_Int32 {
    buf := make([]byte, 1024)
    recv := &DataBlock2{bufferLen: 1024, numBytes: 1024, bufferPtr: unsafe.Pointer(&buf[0])}
    ret := BSA_Int32(C.BSAGetData((C.Handle_Index)(bsaHandle),
                                  (*C.DataBlock)(unsafe.Pointer(recv))))
    *dataBlockPtr = *convertDataBlock2(recv)
    return ret
}

func BSAEndData(bsaHandle int64) BSA_Int32 {
    return BSA_Int32(C.BSAEndData((C.Handle_Index)(bsaHandle)))
}

func BSAEndTxn(bsaHandle Handle_Index, vote Vote) BSA_Int32 {
    return BSA_Int32(C.BSAEndTxn((C.Handle_Index)(bsaHandle),
                                 (C.Vote)(vote)))
}

func BSATerminate(bsaHandle Handle_Index) BSA_Int32 {
    return BSA_Int32(C.BSATerminate((C.Handle_Index)(bsaHandle)))
}