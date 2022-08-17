package app

//对象元数据
type ObjMetadata struct {
	Name              string      `json:"name"`              //对象名称(文件名称)
	ContentLength     uint64      `json:"contentLength"`     //文件大小，单位是byte
	ContentType       string      `json:"contentType"`       //文件类型
	ContentEncode     string      `json:"contentEncode"`     //编码方式
	Suffix            string      `json:"suffix"`            //后缀
	UploadTime        uint64      `json:"uploadTime"`        //文件的上传时间的时间戳(秒级别)
	Md5               string      `json:"md5"`               //上传时客户端计算的Md5的值
	SliceInfo         []SliceMeta `json:"sliceInfo"`         //分片信息，可确定分片的数量以及每一分片的大小
	OriDataSliceCount uint32      `json:"oriDataSliceCount"` //存储原始数据的分片数量
	ECCodeSliceCount  uint32      `json:"eccodeSliceCount"`  //存储EC码数据的分片数量
	IsEncript         uint32      `json:"isEncript"`         //是否进行了加密,0表示没有，1表示加密了
	EncriptAlgo       string      `json:"encriptAlgo"`       //加密算法
	Expire            uint64      `json:"expire"`            //将在expire时间后(Duration单位)过期
}

//数据分片的类型：实际数据或者EC码
type SliceType uint8

const (
	SliceData   SliceType = iota //对象实际数据
	SliceECCode                  //对象的EC码
)

//每个分片的元数据信息
type SliceMeta struct {
	Num    uint32    `json:"num"`    //分片的编号
	Length uint64    `json:"length"` //分片的长度
	Type   SliceType `json:"type"`   //分片的类型：数据类型或者EC码类型
	Md5    string    `json:"md5"`
}

//对象元数据和数据分片的上传分别使用两个不同的数据结构，不复用
//对象元数据的上传请求
type ObjectMetadataUploadReq struct {
	UserId     string      `json:"userId"`     //用户id
	BucketName string      `json:"bucketName"` //桶名称
	ObjName    string      `json:"objName"`    //对象名称
	MetaInfo   ObjMetadata `json:"metaInfo"`   //对象的元数据信息
}

//对象元数据上传请求的回复
type ObjectMetadataUploadRsp struct {
	Ret      int    `json:"ret"`      //回复状态码
	ErrorMsg string `json:"errorMsg"` //具体错误信息
	TaskId   string `json:"taskId"`   //对象唯一id
}

//分片数据信息
type ObjSlice struct {
	Type SliceType `json:"type"` //对象类型，可以是实际存储对象或者EC码
	Num  uint32    `json:"num"`  //编号(用于按序恢复原对象)
	Data []byte    `json:"data"` //分片数据
}

//发送数据分片的请求实体,由client发送给dispatch
type ObjectSliceUploadReq struct {
	UserId     string              `json:"userId"`     //用户id
	BucketName string              `json:"bucketName"` //桶名称
	ObjName    string              `json:"objName"`    //对象名称
	Slice      map[uint32]ObjSlice `json:"slice"`      //分片的数据,key是分片的编号
}

//获取对象的分片落地方案的请求：dispatch向dataHandle发送
type ObjectStoragePlanReq struct {
	ObjectId string `json:"objectId"` //所请求对象的id
}

//对象的落地方案的回复，dataHandle发送给dispatch
type ObjectStoragePlanRsp struct {
	Ret           int                   `json:"ret"`        //回复状态码：标记了落盘的结果，如果是ok，会有相应的分片数据由dispatch转发给相应client，如果是error，则不会有
	ErrorMsg      string                `json:"errorMsg"`   //具体错误信息
	UserId        string                `json:"userId"`     //用户id
	BucketName    string                `json:"bucketName"` //桶名称
	ObjName       string                `json:"objName"`    //对象名称
	Times         uint8                 `json:"times"`      //该对象的第几次方案
	SliceLocation []ObjectSliceLocation `json:"sliceLocation"`
}

//每一个分片的落地方案
type ObjectSliceLocation struct {
	ObjectId  string    `json:"objectId"`  //对象id
	Num       uint32    `json:"num"`       //编号(用于按序恢复原对象)
	NodeId    uint64    `json:"nodeId"`    //所落节点,
	Type      SliceType `json:"type"`      //对象类型，可以是实际存储对象或者EC码
	BlockPath string    `json:"blockPath"` //所落block的路径
	OffSet    uint64    `json:"offset"`    //在文件中的偏移
	Length    uint64    `json:"length"`    //在文件中的大小，单位是byte
	Md5       string    `json:"md5"`
}

//dispatch根据落地方案和分片数据构造的发送给node的待存储数据
type ObjSliceSaveReq struct {
	ObjectId  string `json:"objectId"`  //对象的id
	NodeId    uint64 `json:"nodeId"`    //被分发到的节点
	Num       uint32 `json:"num"`       //编号(用于按序恢复原对象)
	Version   int32  `json:"version"`   //版本
	BlockPath string `json:"blockPath"` //所落的文件路径
	Offset    uint64 `json:"offset"`    //在文件中的偏移
	Data      []byte `json:"data"`      //分片数据
}

//dataSaveSvr在将分片数据落盘后，对dispatchSvr的回复
type ObjSliceSaveRsp struct {
	Ret      int    `json:"ret"`      //回复状态码：标记了落盘的结果，如果是ok，会有相应的分片数据由dispatch转发给相应client，如果是error，则不会有
	ErrorMsg string `json:"errorMsg"` //具体错误信息
	ObjectId string `json:"objectId"` //对象id
	Num      uint32 `json:"num"`      //分片编号
	NodeId   uint64 `json:"nodeId"`   //被分发到的节点
	Version  int32  `json:"version"`  //分片落地方案的版本
}

//分片落地结果的回复:由dispatchSvr根据ataSaveSvr的回复结果汇总得出，然后由dispatch发往dataHandle
type ObjectSaveRsp struct {
	Ret          int               `json:"ret"`          //回复状态码
	ErrorMsg     string            `json:"errorMsg"`     //具体错误信息
	ObjectId     string            `json:"objectId"`     //对象id
	SliceSaveRes []ObjSliceSaveRes `json:"sliceSaveRes"` //每一个分片的落盘结果
}

//如果有分片落地失败，会相应的由dispatchSvr生成一项ObjSliceSaveRes，放入ObjectSaveRsp中
type ObjSliceSaveRes struct {
	Ret    int    `json:"ret"`    //1表示成功，否则表示失败
	Num    uint32 `json:"num"`    //分片编号
	NodeId uint64 `json:"nodeId"` //被分发到的节点
}

//对象上传的最终结果，DataHandle发往DispatchSvr
//发送该数据，表示对象的上传全流程已经结束
type ObjectUploadRsp struct {
	Ret      int    `json:"ret"`      //回复状态码
	ErrorMsg string `json:"errorMsg"` //具体错误信息
	ObjectId string `json:"objectId"`
}

//对象的下载、删除的请求
type ObjectReq struct {
	UserId     string `json:"userId"`     //用户id
	BucketName string `json:"bucketName"` //桶名称
	ObjName    string `json:"objName"`    //对象名称
}

//client向dispatchSvr发出对象下载请求后，dispatchSvr将向dataHandle发送获取元数据以及分片落地方案记录的请求
//此为此请求的回复：元数据和分片一起发送过来，dispatchSvr会先将元数据发送回client,然后向dataSaveSvr请求实际数据进而转发给client
type ObjectLocationRsp struct {
	Ret              int                   `json:"ret"`          //回复状态码
	ErrorMsg         string                `json:"errorMsg"`     //具体错误信息
	UserId           string                `json:"userId"`       //用户id
	BucketName       string                `json:"bucketName"`   //桶名称
	ObjectName       string                `json:"objName"`      //对象名称
	ObjMetadata      ObjMetadata           `json:"objMetaData"`  //对象的元数据信息
	ObjSliceLocation []ObjectSliceLocation `json:"objSliceData"` //对象的分片数据
}

//dataSaveSvr读取数据后，将数据经dispatch转发给client，同时会向dispatch发送读取数据的结果
type ObjSliceGetRsp struct {
	Ret      int       `json:"ret"`      //回复状态码
	ErrorMsg string    `json:"errorMsg"` //具体错误信息
	ObjectId string    `json:"objectId"` //对象id
	Num      uint32    `json:"num"`      //编号(用于按序恢复原对象)
	NodeId   uint64    `json:"nodeId"`   //所在节点
	Type     SliceType `json:"type"`     //对象类型，可以是实际存储对象或者EC码
}

//发送给client的查询结果的回复中，数据的类型：元数据或者分片数据
type QueryDataType uint8

const (
	ObjectMetaData  QueryDataType = iota //对象的元数据
	ObjectSliceData                      //对象的分片数据
)

//dataSaveSvr读取数据后，将数据经dispatchSvr转发给client，
type ObjectQueryRsp struct {
	Ret      int           `json:"ret"`      //回复状态码
	ErrorMsg string        `json:"errorMsg"` //具体错误信息
	DataType QueryDataType `json:"dataType"` //数据的类型
	Meta     ObjMetadata   `json:"meta"`     //原数据信息
	Slice    ObjSlice      `json:"slice"`    //分片数据
}

//删除请求
type ObjectDeleteRsp struct {
	Ret      int    `json:"ret"`      //回复状态码
	ErrorMsg string `json:"errorMsg"` //具体错误信息
}
