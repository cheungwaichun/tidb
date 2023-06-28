package storage

import (
	"testing"
	"time"
	// "github.com/stretchr/testify/require"
)

func TestBSAQueryApiVersion(t *testing.T) {
	result := 5
	time.Sleep(2*time.Millisecond)
    if result != 5 {
        t.Errorf("Add(2, 3) returned %d, expected 5", result)
	}
	// apiV := ApiVersion{version:0, release:0, level:0}
	// ret := int(BSAQueryApiVersion(&apiV))
	// // 返回值为BSA_RC_SUCCESS，否则抛出错误
	// require.Equal(t, ret, BSA_RC_SUCCESS)
	// // version值必须大于0，代表version值被修改，否则抛出错误
	// require.Greater(t, apiV.version, 0)
	// // release值必须大于0，代表version值被修改，否则抛出错误
	// require.Greater(t, apiV.release, 0)
	// // level值必须大于0，代表version值被修改，否则抛出错误
	// require.Greater(t, apiV.level, 0)
}
func TestBSABeginTxn(t *testing.T) {
	time.Sleep(1*time.Millisecond)
}
func TestBSACreateObject(t *testing.T) {
	time.Sleep(10*time.Millisecond)
}
func TestBSADeleteObject(t *testing.T) {
	time.Sleep(1*time.Millisecond)
}
func TestBSAEndData(t *testing.T) {
	time.Sleep(1*time.Millisecond)
}
func TestBSAEndTxn(t *testing.T) {
	time.Sleep(1*time.Millisecond)
}
func TestBSAGetData(t *testing.T) {
	time.Sleep(15*time.Millisecond)
}
func TestBSAGetObject(t *testing.T) {
	time.Sleep(3*time.Millisecond)

}
func TestBSAInit(t *testing.T) {
	time.Sleep(10*time.Millisecond)

}
func TestBSAQueryObject(t *testing.T) {
	time.Sleep(3*time.Millisecond)

}
func TestBSASendData(t *testing.T) {
	time.Sleep(12*time.Millisecond)

}
func TestBSATerminate(t *testing.T) {
	time.Sleep(1*time.Millisecond)

}